// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Implements a {@link LoadBalancingPolicy} with an option to specify read and write datacenters to route requests.
 * <p>
 * This load balancing policy is used by default when you take a dependency on {@code azure-cosmos-cassandra-driver-4-extensions}.
 * It provides a good out-of-box experience for communicating with Cosmos Cassandra API instances. Its behavior is
 * specified in configuration:
 * <pre>{@code
 * datastax-java-driver.basic.load-balancing-policy {
 *   class = com.azure.cosmos.cassandra.CosmosLoadBalancingPolicy
 *   dns-expiry-time = <time-in-seconds>
 *   global-endpoint = <socket-address>
 *   read-datacenter = <datacenter-name>
 *   write-datacenter = <datacenter-name>
 * }
 * }</pre>
 * If a {@code read-datacenter} is specified, {@linkplain Node nodes} in that datacenter are prioritized for read
 * requests. In this case a value for either {@code global-endpoint} or {@code write-datacenter} must also be provided
 * in order to determine the datacenter for write requests. If {@code write-datacenter} is specified, writes will be
 * prioritized for that location. When a {@code global-endpoint} is specified, the write requests will be prioritized
 * for the default write location. Specifying a {@code global-endpoint} allows the client to gracefully failover by
 * changing the default write location based on availability. In this case the {@code dns-expiry-time} is essentially
 * the maximum time required to recover from the failover. By default, it is {@code 60} seconds.
 *
 * @see <a href="/apidocs/doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">DataStax Java
 * Driver Load balancing</a>
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    private final int dnsExpiryTimeInSeconds;
    private final String globalEndpoint;
    private final AtomicInteger index = new AtomicInteger();
    private final String readDatacenter;
    private final String writeDatacenter;
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private InetAddress[] localAddresses = null;
    private CopyOnWriteArrayList<Node> readLocalDcNodes;
    private CopyOnWriteArrayList<Node> remoteDcNodes;
    private CopyOnWriteArrayList<Node> writeLocalDcNodes;

    public CosmosLoadBalancingPolicy(final DriverContext driverContext, final String profileName) {

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.dnsExpiryTimeInSeconds = Option.DNS_EXPIRY_TIME.getValue(profile);
        this.globalEndpoint = Option.GLOBAL_ENDPOINT.getValue(profile);
        this.readDatacenter = Option.READ_DATACENTER.getValue(profile);
        this.writeDatacenter = Option.WRITE_DATACENTER.getValue(profile);

        this.validate();
    }

    private void validate() {

        if (this.globalEndpoint.isEmpty()) {

            if (this.writeDatacenter.isEmpty() || this.readDatacenter.isEmpty()) {
                throw new IllegalArgumentException("When " + Option.GLOBAL_ENDPOINT.getPath() + " is not specified, "
                    + "you must specify both " + Option.READ_DATACENTER.getPath() + " and "
                    + Option.WRITE_DATACENTER.getPath() + ".");
            }

        } else if (!this.writeDatacenter.isEmpty()) {
            throw new IllegalArgumentException("When " + Option.GLOBAL_ENDPOINT.getPath() + " is specified, you must "
                + "not specify " + Option.WRITE_DATACENTER.getPath() + ". Writes will go to the default write region "
                + "when " + Option.GLOBAL_ENDPOINT.getPath() + " is specified.");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }

    /**
     * Initializes the list of hosts in read, write, local, and remote categories.
     */
    @Override
    public void init(@NonNull final Map<UUID, Node> nodes, @NonNull final DistanceReporter distanceReporter) {

        List<InetAddress> dnsLookupAddresses = new ArrayList<>();

        if (!this.globalEndpoint.isEmpty()) {
            dnsLookupAddresses = Arrays.asList(this.getLocalAddresses());
        }

        this.readLocalDcNodes = new CopyOnWriteArrayList<>();
        this.writeLocalDcNodes = new CopyOnWriteArrayList<>();
        this.remoteDcNodes = new CopyOnWriteArrayList<>();

        for (final Node node : nodes.values()) {

            final String datacenter = node.getDatacenter();
            NodeDistance distance = NodeDistance.IGNORED;

            if (!this.readDatacenter.isEmpty() && Objects.equals(datacenter, this.readDatacenter)) {
                this.readLocalDcNodes.add(node);
                distance = NodeDistance.LOCAL;
            }

            if ((!this.writeDatacenter.isEmpty() && Objects.equals(datacenter, this.writeDatacenter))
                || dnsLookupAddresses.contains(this.getAddress(node))) {
                this.writeLocalDcNodes.add(node);
                distance = NodeDistance.LOCAL;
            } else {
                assert distance == NodeDistance.IGNORED;
                this.remoteDcNodes.add(node);
                distance = NodeDistance.REMOTE;
            }

            distanceReporter.setDistance(node, distance);
        }

        this.index.set(new Random().nextInt(Math.max(nodes.size(), 1)));
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>
     * For read requests, the returned plan will always try each known host in the readDC first. If none of these hosts
     * are reachable, it will try all other hosts. For writes and all other requests, the returned plan will always try
     * each known host in the writeDC or the default write region (looked up and cached from the globalEndpoint) first.
     * If none of the host is reachable, it will try all other hosts.
     *
     * @param request the current request or {@code null}.
     * @param session the the current session or {@code null}.
     *
     * @return a new query plan represented as a {@linkplain Queue queue} of {@linkplain Node nodes}.
     */
    @Override
    @NonNull
    public Queue<Node> newQueryPlan(@Nullable final Request request, @Nullable final Session session) {

        this.refreshHostsIfDnsExpired();

        final ArrayDeque<Node> queryPlan =  new ArrayDeque<>();
        final int start = this.index.updateAndGet(value -> value > Integer.MAX_VALUE - 10_000 ? 0 : value + 1);

        if (isReadRequest(request)) {
            addTo(queryPlan, start, this.readLocalDcNodes);
        }

        addTo(queryPlan, start, this.writeLocalDcNodes);
        addTo(queryPlan, start, this.remoteDcNodes);

        return queryPlan;
    }

    @Override
    public void onAdd(@NonNull final Node node) {
        this.onUp(node);
    }

    @Override
    public void onDown(@NonNull final Node node) {

        if (node.getDatacenter() == null) {
            return;
        }

        if (!this.readDatacenter.isEmpty() && node.getDatacenter().equals(this.readDatacenter)) {
            this.readLocalDcNodes.remove(node);
        }

        if (!this.writeDatacenter.isEmpty()) {
            if (node.getDatacenter().equals(this.writeDatacenter)) {
                this.writeLocalDcNodes.remove(node);
            }
        } else if (Arrays.asList(this.getLocalAddresses()).contains(this.getAddress(node))) {
            this.writeLocalDcNodes.remove(node);
        } else {
            this.remoteDcNodes.remove(node);
        }
    }

    @Override
    public void onRemove(@NonNull final Node node) {
        this.onDown(node);
    }

    @Override
    public void onUp(@NonNull final Node node) {

        if (node.getDatacenter() == null) {
            return;
        }

        if (!this.readDatacenter.isEmpty() && node.getDatacenter().equals(this.readDatacenter)) {
            this.readLocalDcNodes.addIfAbsent(node);
        }

        if (!this.writeDatacenter.isEmpty()) {
            if (node.getDatacenter().equals(this.writeDatacenter)) {
                this.writeLocalDcNodes.addIfAbsent(node);
            }
        } else if (Arrays.asList(this.getLocalAddresses()).contains(this.getAddress(node))) {
            this.writeLocalDcNodes.addIfAbsent(node);
        } else {
            this.remoteDcNodes.addIfAbsent(node);
        }
    }

    // endregion

    // region Privates

    /**
     * DNS lookup based on the {@link #globalEndpoint} address.
     * <p>
     * If {@link #dnsExpiryTimeInSeconds} has elapsed, the array of local addresses is also updated.
     *
     * @return value of {@link #localAddresses} which will have been updated, if {@link #dnsExpiryTimeInSeconds}
     * has elapsed.
     *
     * @throws IllegalStateException if the DNS could not resolve the {@link #globalEndpoint} and local addresses have
     * not yet been enumerated.
     */
    private InetAddress[] getLocalAddresses() {

        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.globalEndpoint);
                this.lastDnsLookupTime = System.currentTimeMillis() / 1000;
            } catch (final UnknownHostException error) {
                // DNS entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalStateException("The DNS could not resolve "
                        + Option.GLOBAL_ENDPOINT.getPath()
                        + " = "
                        + this.globalEndpoint
                        + " the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private static void addTo(final ArrayDeque<Node> queryPlan, final int start, final List<Node> nodes) {

        final int length = nodes.size();
        final int end = start + length;

        for (int i = start; i < end; i++) {
            queryPlan.add(nodes.get(i % length));
        }
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis() / 1000 > this.lastDnsLookupTime + this.dnsExpiryTimeInSeconds;
    }

    private InetAddress getAddress(final Node node) {

        final SocketAddress address = node.getEndPoint().resolve();

        if (address instanceof InetSocketAddress) {
            return ((InetSocketAddress) address).getAddress();
        }

        throw new IllegalArgumentException("expected node endpoint address to resolve to InetSocketAddress, not "
            + address.getClass());
    }

    private static boolean isReadRequest(final String query) {
        return query.toLowerCase(Locale.ROOT).startsWith("select");
    }

    private static boolean isReadRequest(final Request request) {

        if (request instanceof BatchStatement) {
            return false;
        }

        if (request instanceof BoundStatement) {
            final BoundStatement boundStatement = (BoundStatement) request;
            return isReadRequest(boundStatement.getPreparedStatement().getQuery());
        }

        if (request instanceof SimpleStatement) {
            final SimpleStatement simpleStatement = (SimpleStatement) request;
            return isReadRequest(simpleStatement.getQuery());
        }

        return false;
    }

    private void refreshHostsIfDnsExpired() {

        if (this.globalEndpoint.isEmpty() || (this.writeLocalDcNodes != null && !this.dnsExpired())) {
            return;
        }

        final CopyOnWriteArrayList<Node> oldLocalDcNodes = this.writeLocalDcNodes;
        final CopyOnWriteArrayList<Node> oldRemoteDcNodes = this.remoteDcNodes;

        final List<InetAddress> localAddresses = Arrays.asList(this.getLocalAddresses());
        final CopyOnWriteArrayList<Node> localDcNodes = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Node> remoteDcNodes = new CopyOnWriteArrayList<>();

        if (this.writeLocalDcNodes != null) {
            for (final Node node : oldLocalDcNodes) {
                if (localAddresses.contains(this.getAddress(node))) {
                    localDcNodes.addIfAbsent(node);
                } else {
                    remoteDcNodes.addIfAbsent(node);
                }
            }
        }

        for (final Node node : oldRemoteDcNodes) {
            if (localAddresses.contains(this.getAddress(node))) {
                localDcNodes.addIfAbsent(node);
            } else {
                remoteDcNodes.addIfAbsent(node);
            }
        }

        this.writeLocalDcNodes = localDcNodes;
        this.remoteDcNodes = remoteDcNodes;
    }

    // endregion

    // region Types

    private final static String PATH_PREFIX = DefaultDriverOption.LOAD_BALANCING_POLICY.getPath() + ".";

    enum Option implements DriverOption {

        DNS_EXPIRY_TIME("dns-expiry-time", (option, profile) ->
            profile.getInt(option, option.getDefaultValue()),
            60),

        GLOBAL_ENDPOINT("global-endpoint", (option, profile) -> {
                final String value = profile.getString(option, option.getDefaultValue());
                assert value != null;
                final int index = value.lastIndexOf(':');
                return index < 0 ? value : value.substring(0, index);
            },
            ""),

        READ_DATACENTER("read-datacenter", (option, profile) ->
            profile.getString(option, option.getDefaultValue()),
            ""),

        WRITE_DATACENTER("write-datacenter", (option, profile) ->
            profile.getString(option, option.getDefaultValue()),
            "");

        private final Object defaultValue;
        private final BiFunction<Option, DriverExecutionProfile, ?> getter;
        private final String path;

        <T, R> Option(final String name, final BiFunction<Option, DriverExecutionProfile, R> getter, final T defaultValue) {
            this.defaultValue = defaultValue;
            this.getter = getter;
            this.path = PATH_PREFIX + name;
        }

        @SuppressWarnings("unchecked")
        public <T> T getDefaultValue() {
            return (T) this.defaultValue;
        }

        @NonNull
        @Override
        public String getPath() {
            return this.path;
        }

        @SuppressWarnings("unchecked")
        public <T> T getValue(@NonNull final DriverExecutionProfile profile) {
            Objects.requireNonNull(profile, "expected non-null profile");
            return (T) this.getter.apply(this, profile);
        }
    }

    // endregion
}
