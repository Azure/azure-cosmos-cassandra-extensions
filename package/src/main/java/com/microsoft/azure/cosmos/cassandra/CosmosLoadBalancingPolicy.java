/*
 * The MIT License (MIT)
 *
 * Copyright (c) Microsoft. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.azure.cosmos.cassandra;

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
 * Either one of {@link #writeDC} or {@link #globalContactPoint} needs to be specified in order to determine the data
 * center for write requests. If {@link #writeDC} is specified, writes will be prioritized for that region. When a
 * {@link #globalContactPoint} is specified, the write requests will be prioritized for the default write region. {@link
 * #globalContactPoint} allows the client to gracefully failover by changing the default write region addresses. {@link
 * #dnsExpirationInSeconds} is essentially the max duration to recover from the failover. By default, it is 60 seconds.
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    private final int dnsExpirationInSeconds;
    private final String globalContactPoint;
    private final AtomicInteger index = new AtomicInteger();
    private final String readDC;
    private final String writeDC;
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private InetAddress[] localAddresses = null;
    private CopyOnWriteArrayList<Node> readLocalDcNodes;
    private CopyOnWriteArrayList<Node> remoteDcNodes;
    private CopyOnWriteArrayList<Node> writeLocalDcNodes;

    private CosmosLoadBalancingPolicy(
        String readDC,
        String writeDC,
        String globalContactPoint,
        int dnsExpirationInSeconds) {

        this.readDC = readDC;
        this.writeDC = writeDC;
        this.globalContactPoint = globalContactPoint;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void close() {
        // nothing to do
    }

    /**
     * Initializes the list of hosts in read, write, local, and remote categories.
     */
    @Override
    public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {

        List<InetAddress> dnsLookupAddresses = new ArrayList<>();

        if (!globalContactPoint.isEmpty()) {
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
        final int start = index.updateAndGet(value -> value > Integer.MAX_VALUE - 10_000 ? 0 : value + 1);

        if (isReadRequest(request)) {
            addTo(queryPlan, start, this.readLocalDcNodes);
        }

        addTo(queryPlan, start, this.writeLocalDcNodes);
        addTo(queryPlan, start, this.remoteDcNodes);

        return queryPlan;
    }

    private static void addTo(ArrayDeque<Node> queryPlan, int start, List<Node> nodes) {

        final int length = nodes.size();
        final int end = start + length;

        for (int i = start; i < end; i++) {
            queryPlan.add(nodes.get(i % length));
        }
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
        } else if (Arrays.asList(getLocalAddresses()).contains(this.getAddress(node))) {
            this.writeLocalDcNodes.remove(node);
        } else {
            this.remoteDcNodes.remove(node);
        }
    }

    @Override
    public void onRemove(@NonNull Node node) {
        onDown(node);
    }

    @Override
    public void onUp(@NonNull Node node) {

        if (node == null || node.getDatacenter() == null) {
            return;
        }

        if (!this.readDC.isEmpty() && node.getDatacenter().equals(this.readDC)) {
            this.readLocalDcNodes.addIfAbsent(node);
        }

        if (!this.writeDC.isEmpty()) {
            if (node.getDatacenter().equals(this.writeDC)) {
                this.writeLocalDcNodes.addIfAbsent(node);
            }
        } else if (Arrays.asList(getLocalAddresses()).contains(this.getAddress(node))) {
            this.writeLocalDcNodes.addIfAbsent(node);
        } else {
            this.remoteDcNodes.addIfAbsent(node);
        }
    }

    /**
     * DNS lookup based on the globalContactPoint and update localAddresses.
     *
     * @return
     */
    private InetAddress[] getLocalAddresses() {

        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.globalContactPoint);
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

    private static CosmosLoadBalancingPolicy buildFrom(Builder builder) {
        return new CosmosLoadBalancingPolicy(builder.readDC, builder.writeDC, builder.globalEndpoint,
            builder.dnsExpirationInSeconds);
    }

    @SuppressWarnings("unchecked")
    private static List<Node> cloneList(CopyOnWriteArrayList<Node> list) {
        return (CopyOnWriteArrayList<Node>) list.clone();
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis() / 1000 > lastDnsLookupTime + dnsExpirationInSeconds;
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

        if (this.globalContactPoint.isEmpty() || (this.writeLocalDcNodes != null && !dnsExpired())) {
            return;
        }

        CopyOnWriteArrayList<Node> oldLocalDCHosts = this.writeLocalDcNodes;
        CopyOnWriteArrayList<Node> oldRemoteDCHosts = this.remoteDcNodes;

        List<InetAddress> localAddresses = Arrays.asList(getLocalAddresses());
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

    public static class Builder {

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
}
