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
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a Cassandra {@link LoadBalancingPolicy} with an option to specify a {@link #readDC} and a {@link #writeDC}
 * to route read and write requests to their corresponding data centers.
 * <p>
 * If {@link #readDC} is specified, we prioritize {@linkplain Node nodes} in the {@link #readDC} for read requests.
 * Either one of {@link #writeDC} or {@link #globalEndpoint} needs to be specified in order to determine the data
 * center for write requests. If {@link #writeDC} is specified, writes will be prioritized for that region. When a
 * {@link #globalEndpoint} is specified, the write requests will be prioritized for the default write region. {@link
 * #globalEndpoint} allows the client to gracefully failover by changing the default write region addresses. {@link
 * #dnsExpiryTimeInSeconds} is essentially the max duration to recover from the failover. By default, it is 60 seconds.
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    private final int dnsExpiryTimeInSeconds;
    private final String globalEndpoint;
    private final AtomicInteger index = new AtomicInteger();
    private final String readDC;
    private final String writeDC;
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private InetAddress[] localAddresses = null;
    private CopyOnWriteArrayList<Node> readLocalDcNodes;
    private CopyOnWriteArrayList<Node> remoteDcNodes;
    private CopyOnWriteArrayList<Node> writeLocalDcNodes;

    public CosmosLoadBalancingPolicy(DriverContext driverContext, String profileName) {

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.dnsExpiryTimeInSeconds = profile.getInt(Option.DNS_EXPIRY_TIME,
            Option.DNS_EXPIRY_TIME.getDefaultValue());

        this.globalEndpoint = profile.getString(Option.GLOBAL_ENDPOINT,
            Option.GLOBAL_ENDPOINT.getDefaultValue());

        this.readDC = profile.getString(Option.READ_DC,
            Option.READ_DC.getDefaultValue());

        this.writeDC = profile.getString(Option.WRITE_DC,
            Option.WRITE_DC.getDefaultValue());
    }

    private CosmosLoadBalancingPolicy(
        String readDC,
        String writeDC,
        String globalContactPoint,
        int dnsExpiryTimeInSeconds) {

        this.readDC = readDC;
        this.writeDC = writeDC;
        this.globalEndpoint = globalContactPoint;
        this.dnsExpiryTimeInSeconds = dnsExpiryTimeInSeconds;
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

            if (!this.readDC.isEmpty() && Objects.equals(datacenter, this.readDC)) {
                this.readLocalDcNodes.add(node);
                distance = NodeDistance.LOCAL;
            }

            if ((!this.writeDC.isEmpty() && Objects.equals(datacenter, this.writeDC))
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

        if (!this.readDC.isEmpty() && node.getDatacenter().equals(this.readDC)) {
            this.readLocalDcNodes.remove(node);
        }

        if (!this.writeDC.isEmpty()) {
            if (node.getDatacenter().equals(this.writeDC)) {
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

        if (!this.readDC.isEmpty() && node.getDatacenter().equals(this.readDC)) {
            this.readLocalDcNodes.addIfAbsent(node);
        }

        if (!this.writeDC.isEmpty()) {
            if (node.getDatacenter().equals(this.writeDC)) {
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

    static Builder builder() {
        return new Builder();
    }

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
                    throw new IllegalArgumentException(
                        "The DNS could not resolve the globalContactPoint the first time.");
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

    private static CosmosLoadBalancingPolicy buildFrom(Builder builder) {
        return new CosmosLoadBalancingPolicy(builder.readDC, builder.writeDC, builder.globalEndpoint,
            builder.dnsExpirationInSeconds);
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
        return query.toLowerCase().startsWith("select");
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

        CopyOnWriteArrayList<Node> oldLocalDCHosts = this.writeLocalDcNodes;
        CopyOnWriteArrayList<Node> oldRemoteDCHosts = this.remoteDcNodes;

        List<InetAddress> localAddresses = Arrays.asList(this.getLocalAddresses());
        CopyOnWriteArrayList<Node> localDcHosts = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Node> remoteDcHosts = new CopyOnWriteArrayList<>();

        for (Node node : oldLocalDCHosts) {
            if (localAddresses.contains(this.getAddress(node))) {
                localDcHosts.addIfAbsent(node);
            } else {
                remoteDcHosts.addIfAbsent(node);
            }
        }

        for (Node node : oldRemoteDCHosts) {
            if (localAddresses.contains(this.getAddress(node))) {
                localDcHosts.addIfAbsent(node);
            } else {
                remoteDcHosts.addIfAbsent(node);
            }
        }

        this.writeLocalDcNodes = localDcHosts;
        this.remoteDcNodes = remoteDcHosts;
    }

    private static void validate(Builder builder) {

        if (builder.globalEndpoint.isEmpty()) {
            if (builder.writeDC.isEmpty() || builder.readDC.isEmpty()) {
                throw new IllegalArgumentException("When the globalEndpoint is not specified, you need to provide " +
                    "both " +
                    "readDC and writeDC.");
            }
        } else {
            if (!builder.writeDC.isEmpty()) {
                throw new IllegalArgumentException("When the globalEndpoint is specified, you can't provide writeDC. " +
                    "Writes will go " +
                    "to the default write region when the globalEndpoint is specified.");
            }
        }
    }

    // endregion

    // region Types

    static class Builder {

        private int dnsExpirationInSeconds = 60;
        private String globalEndpoint = "";
        private String readDC = "";
        private String writeDC = "";

        public CosmosLoadBalancingPolicy build() {
            validate(this);
            return CosmosLoadBalancingPolicy.buildFrom(this);
        }

        public Builder withDnsExpirationInSeconds(int dnsExpirationInSeconds) {
            this.dnsExpirationInSeconds = dnsExpirationInSeconds;
            return this;
        }

        public Builder withGlobalEndpoint(String globalEndpoint) {
            this.globalEndpoint = globalEndpoint;
            return this;
        }

        public Builder withReadDC(String readDC) {
            this.readDC = readDC;
            return this;
        }

        public Builder withWriteDC(String writeDC) {
            this.writeDC = writeDC;
            return this;
        }
    }

    private enum Option implements DriverOption {

        DNS_EXPIRY_TIME(DefaultDriverOption.LOAD_BALANCING_POLICY + "dns-expiry-time",60 ),
        GLOBAL_ENDPOINT(DefaultDriverOption.LOAD_BALANCING_POLICY + ".global-endpoint", ""),
        READ_DC(DefaultDriverOption.LOAD_BALANCING_POLICY + ".read-dc", ""),
        WRITE_DC(DefaultDriverOption.LOAD_BALANCING_POLICY + ".write-dc", "");

        private final Object defaultValue;
        private final String path;

        Option(String path, Object defaultValue) {
            this.path = path;
            this.defaultValue = defaultValue;
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
    }

    // endregion
}
