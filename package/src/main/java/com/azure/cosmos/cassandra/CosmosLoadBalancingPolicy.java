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
 * Implements a Cassandra {@link LoadBalancingPolicy} with an option to specify a {@link #readDatacenter} and a
 * {@link #writeDatacenter} to route read and write requests to their corresponding datacenters.
 * <p>
 * If {@link #readDatacenter} is specified, we prioritize {@linkplain Node nodes} in the {@link #readDatacenter} for
 * read requests. Either one of {@link #writeDatacenter} or {@link #globalEndpoint} needs to be specified in order to
 * determine the datacenter for write requests. If {@link #writeDatacenter} is specified, writes will be prioritized for
 * that region. When a {@link #globalEndpoint} is specified, the write requests will be prioritized for the default
 * write region. {@link #globalEndpoint} allows the client to gracefully failover by changing the default write region
 * addresses. {@link #dnsExpiryTimeInSeconds} is essentially the max duration to recover from the failover. By default,
 * it is 60 seconds.
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

    public CosmosLoadBalancingPolicy(DriverContext driverContext, String profileName) {

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
                    + "you must specify both " + Option.READ_DATACENTER.getPath() + " and " + Option.WRITE_DATACENTER.getPath()
                    + ".");
            }

        } else if (!this.writeDatacenter.isEmpty()) {
            throw new IllegalArgumentException("When " + Option.GLOBAL_ENDPOINT.getPath() + " is specified, you must "
                + "not specify " + Option.WRITE_DATACENTER.getPath() + ". Writes will go to the default write region when "
                + Option.GLOBAL_ENDPOINT.getPath() + " is specified.");
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
    public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {

        List<InetAddress> dnsLookupAddresses = new ArrayList<>();

        if (!this.globalEndpoint.isEmpty()) {
            dnsLookupAddresses = Arrays.asList(this.getLocalAddresses());
        }

        this.readLocalDcNodes = new CopyOnWriteArrayList<>();
        this.writeLocalDcNodes = new CopyOnWriteArrayList<>();
        this.remoteDcNodes = new CopyOnWriteArrayList<>();

        for (Node node : nodes.values()) {

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
    public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {

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
    public void onAdd(@NonNull Node node) {
        this.onUp(node);
    }

    @Override
    public void onDown(@NonNull Node node) {

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
    public void onRemove(@NonNull Node node) {
        this.onDown(node);
    }

    @Override
    public void onUp(@NonNull Node node) {

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
     * DNS lookup based on the globalContactPoint and update localAddresses.
     *
     * @return updated array of local addresses corresponding to the globalContactPoint.
     */
    private InetAddress[] getLocalAddresses() {

        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.globalEndpoint);
                this.lastDnsLookupTime = System.currentTimeMillis() / 1000;
            } catch (UnknownHostException error) {
                // DNS entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalArgumentException("The DNS could not resolve "
                        + Option.GLOBAL_ENDPOINT.getPath()
                        + " = "
                        + this.globalEndpoint
                        + " the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private static void addTo(ArrayDeque<Node> queryPlan, int start, List<Node> nodes) {

        final int length = nodes.size();
        final int end = start + length;

        for (int i = start; i < end; i++) {
            queryPlan.add(nodes.get(i % length));
        }
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis() / 1000 > this.lastDnsLookupTime + this.dnsExpiryTimeInSeconds;
    }

    private InetAddress getAddress(Node node) {

        // TODO (DANOBLE) Under what circumstances might a cast from InetAddress to InetSocketAddress fail?
        //   EndPoint.resolve returns an InetAddress which--in the case of the standard Datastax EndPoint types--
        //   can safely be be cast to an InetSocketAddress. Derived types--e.g., test types--might not return an
        //   InetSocketAddress. The question therefore becomes: What should we do when we find that the InetAddress
        //   cannot be cast to an InetSocketAddress?

        return ((InetSocketAddress) node.getEndPoint().resolve()).getAddress();
    }

    private static boolean isReadRequest(String query) {
        return query.toLowerCase(Locale.ROOT).startsWith("select");
    }

    private static boolean isReadRequest(Request request) {

        if (request instanceof BatchStatement) {
            return false;
        }

        if (request instanceof BoundStatement) {
            BoundStatement boundStatement = (BoundStatement) request;
            return isReadRequest(boundStatement.getPreparedStatement().getQuery());
        }

        if (request instanceof SimpleStatement) {
            SimpleStatement simpleStatement = (SimpleStatement) request;
            return isReadRequest(simpleStatement.getQuery());
        }

        return false;
    }

    private void refreshHostsIfDnsExpired() {

        if (this.globalEndpoint.isEmpty() || (this.writeLocalDcNodes != null && !this.dnsExpired())) {
            return;
        }

        CopyOnWriteArrayList<Node> oldLocalDcNodes = this.writeLocalDcNodes;
        CopyOnWriteArrayList<Node> oldRemoteDcNodes = this.remoteDcNodes;

        List<InetAddress> localAddresses = Arrays.asList(this.getLocalAddresses());
        CopyOnWriteArrayList<Node> localDcNodes = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Node> remoteDcNodes = new CopyOnWriteArrayList<>();

        if (this.writeLocalDcNodes != null) {
            for (Node node : oldLocalDcNodes) {
                if (localAddresses.contains(this.getAddress(node))) {
                    localDcNodes.addIfAbsent(node);
                } else {
                    remoteDcNodes.addIfAbsent(node);
                }
            }
        }

        for (Node node : oldRemoteDcNodes) {
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

        <T, R> Option(String name, BiFunction<Option, DriverExecutionProfile, R> getter, T defaultValue) {
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
        public <T> T getValue(@NonNull DriverExecutionProfile profile) {
            Objects.requireNonNull(profile, "expected non-null profile");
            return (T) this.getter.apply(this, profile);
        }
    }

    // endregion
}
