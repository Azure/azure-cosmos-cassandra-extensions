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
<<<<<<< HEAD
import com.datastax.oss.driver.api.core.metadata.EndPoint;
=======
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
<<<<<<< HEAD
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
=======
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
<<<<<<< HEAD
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
=======
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
<<<<<<< HEAD
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
=======
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98

/**
 * A {@link LoadBalancingPolicy} implementation with an option to specify read and write datacenters to route requests.
 * <p>
 * This is the default load-balancing policy when you take a dependency on this package. It provides a good out-of-box
 * experience for communicating with Cosmos Cassandra API instances. Its behavior is specified in configuration:
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
<<<<<<< HEAD
 * requests. To determine the datacenter for write requests, a value for either {@code global-endpoint} or {@code
 * write-datacenter} must then be provided. If {@code write-datacenter} is specified, writes will be prioritized for
 * that location. When a {@code global-endpoint} is specified, write requests will be prioritized for the default write
 * location.
=======
 * requests. To determine the datacenter for write requests, a value for either {@code global-endpoint} or
 * {@code write-datacenter} must then be provided. If {@code write-datacenter} is specified, writes will be prioritized
 * for that location. When a {@code global-endpoint} is specified, write requests will be prioritized for the default
 * write location.
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
 * <p>
 * Specifying a {@code global-endpoint} allows the client to gracefully failover by changing the default write location.
 * In this case the {@code dns-expiry-time} is essentially the maximum time required to recover from the failover. By
 * default, it is {@code 60} seconds.
 *
 * @see <a href="../../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">DataStax Java
 * Driver Load balancing</a>
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    // region Fields

<<<<<<< HEAD
    private static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicy.class);
    private static final String OPTION_PATH_PREFIX = DefaultDriverOption.LOAD_BALANCING_POLICY.getPath() + ".";

    private final int dnsExpiryTimeInSeconds;
    private final List<InetAddress> dnsLookupAddresses;
    private final String globalEndpoint;
    private final AtomicInteger index;
    private final Object lock;
    private final String readDatacenter;
    private final String writeDatacenter;

    private DistanceReporter distanceReporter;
    private volatile long lastDnsLookupTime;

    @SuppressFBWarnings("VO_VOLATILE_REFERENCE_TO_ARRAY")
    private volatile InetAddress[] localAddresses;

    private volatile List<Node> localNodesForReading;
    private volatile List<Node> localNodesForWriting;
    private volatile List<Node> remoteNodes;
=======
    private static final String PATH_PREFIX = DefaultDriverOption.LOAD_BALANCING_POLICY.getPath() + ".";

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
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98

    // endregion

    // region Constructors

    /**
     * Initializes a newly created {@link CosmosLoadBalancingPolicy} object from configuration.
     *
     * @param driverContext an object holding the context of the current driver instance.
     * @param profileName   name of the configuration profile to apply.
     */
    public CosmosLoadBalancingPolicy(final DriverContext driverContext, final String profileName) {

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.dnsExpiryTimeInSeconds = Option.DNS_EXPIRY_TIME.getValue(profile);
        this.globalEndpoint = Option.GLOBAL_ENDPOINT.getValue(profile);
        this.readDatacenter = Option.READ_DATACENTER.getValue(profile);
        this.writeDatacenter = Option.WRITE_DATACENTER.getValue(profile);

<<<<<<< HEAD
        this.validateConfiguration();

        this.lastDnsLookupTime = Long.MAX_VALUE;
        this.index = new AtomicInteger();
        this.localAddresses = null;
        this.lock = new Object();

        this.dnsLookupAddresses = Collections.unmodifiableList(this.globalEndpoint.isEmpty()
            ? new ArrayList<>()
            : Arrays.asList(this.getLocalAddresses()));
    }

    // endregion

    // region Accessors

    /**
     * Gets the DNS expiry time in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     *
     * @return the DNS expiry time in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     */
    public int getDnsExpiryTimeInSeconds() {
        return this.dnsExpiryTimeInSeconds;
    }

    /**
     * Gets the global endpoint in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     *
     * @return the global endpoint in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     */
    public String getGlobalEndpoint() {
        return this.globalEndpoint;
    }

    /**
     * Gets the read datacenter in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     *
     * @return the read datacenter in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     */
    public String getReadDatacenter() {
        return this.readDatacenter;
    }

    /**
     * Gets the write datacenter in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     *
     * @return the write datacenter in use by this {@link CosmosLoadBalancingPolicy CosmosLoadBalancingPolicy} object.
     */
    public String getWriteDatacenter() {
        return this.writeDatacenter;
=======
        this.validate();
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
    }

    // endregion

    // region Methods

    /**
     * Closes the current {@link CosmosLoadBalancingPolicy} instance.
     */
    @Override
    public void close() {
        // nothing to do
    }

    /**
     * Initializes the list of hosts in read, write, local, and remote categories.
     *
<<<<<<< HEAD
     * @param nodes            the nodes to be examined.
=======
     * @param nodes the nodes to be examined.
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
     * @param distanceReporter an object that the policy uses to signal decisions it makes about node distances.
     */
    @Override
    public void init(@NonNull final Map<UUID, Node> nodes, @NonNull final DistanceReporter distanceReporter) {

<<<<<<< HEAD
        if (LOG.isDebugEnabled()) {
            LOG.debug("init({})", toString(nodes.values()));
        }

        this.distanceReporter = distanceReporter;
        this.localNodesForReading = new ArrayList<>();
        this.remoteNodes = new ArrayList<>();
        this.localNodesForWriting = new ArrayList<>();

        for (final Node node : nodes.values()) {
            this.reportDistance(node);
        }

        this.index.set(new Random().nextInt(Math.max(nodes.size(), 1)));
        LOG.debug("-> init({})", this);
    }

    /**
     * Returns an ordered list of coordinators to use for a new query.
     * <p>
     * For read requests, the coordinators are ordered to ensure that each known host in the read datacenter is tried
     * first. If none of these coordinators are reachable, all other hosts will be tried. For writes and all other
     * requests, the coordinators are ordered to ensure that each known host in the write datacenter or the default
     * write location (looked up and cached from the global-endpoint) are tried first. If none of these coordinators are
     * reachable, all other hosts will be tried.
     *
     * @param request the current request or {@code null}.
     * @param session the current session or {@code null}.
     *
     * @return a new query plan represented as a {@linkplain Queue queue} of {@linkplain Node nodes}. The queue is a
     * concurrent queue as required by the {@link LoadBalancingPolicy#newQueryPlan} contract.
=======
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
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
     */
    @Override
    @NonNull
    public Queue<Node> newQueryPlan(@Nullable final Request request, @Nullable final Session session) {

<<<<<<< HEAD
        if (LOG.isDebugEnabled()) {
            if (request == null) {
                LOG.debug("newQueryPlan({request=null, session={}})", session == null ? null : session.getName());
            } else {
                LOG.debug("newQueryPlan({request={node:{},keyspace={}},session={}})",
                    toString(request.getNode()),
                    request.getKeyspace(),
                    session == null ? "null" : session.getName());
            }
        }

        this.refreshNodesIfDnsExpired();

        final int start = this.index.updateAndGet(value -> value > Integer.MAX_VALUE - 10_000 ? 0 : value + 1);
        final ConcurrentLinkedQueue<Node> queryPlan = new ConcurrentLinkedQueue<>();
        final boolean readRequest = isReadRequest(request);

        if (readRequest) {
            addTo(queryPlan, this.localNodesForReading, start);
        }

        addTo(queryPlan, this.localNodesForWriting, start);
        addTo(queryPlan, this.remoteNodes, start);

        if (LOG.isDebugEnabled()) {
            LOG.debug("-> newQueryPlan({read-request={}, query-plan={}})", readRequest, toString(queryPlan));
        }
=======
        this.refreshHostsIfDnsExpired();

        final ArrayDeque<Node> queryPlan =  new ArrayDeque<>();
        final int start = this.index.updateAndGet(value -> value > Integer.MAX_VALUE - 10_000 ? 0 : value + 1);

        if (isReadRequest(request)) {
            addTo(queryPlan, start, this.readLocalDcNodes);
        }

        addTo(queryPlan, start, this.writeLocalDcNodes);
        addTo(queryPlan, start, this.remoteDcNodes);
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98

        return queryPlan;
    }

    @Override
    public void onAdd(@NonNull final Node node) {
<<<<<<< HEAD

        if (LOG.isDebugEnabled()) {
            LOG.debug("onAdd({})", toString(node));
        }

        synchronized (this.lock) {
            this.reportDistance(node);
        }

        LOG.debug("-> onAdd({})", this);
=======
        this.onUp(node);
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
    }

    @Override
    public void onDown(@NonNull final Node node) {
<<<<<<< HEAD
        if (LOG.isDebugEnabled()) {
            LOG.debug("onDown({})", toString(node));
        }
    }

    @Override
    public void onRemove(@NonNull final Node node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onRemove({})", toString(node));
        }

        final String datacenter = node.getDatacenter();

        if (datacenter != null) {

            synchronized (this.lock) {

                if (!this.readDatacenter.isEmpty() && datacenter.equals(this.readDatacenter)) {
                    removeFrom(this.localNodesForReading, node);
                }

                if (!this.writeDatacenter.isEmpty()) {
                    if (datacenter.equals(this.writeDatacenter)) {
                        this.localNodesForWriting.remove(node);
                    }
                } else if (Arrays.asList(this.getLocalAddresses()).contains(this.getAddress(node))) {
                    removeFrom(this.localNodesForWriting, node);
                } else {
                    removeFrom(this.remoteNodes, node);
                }
            }
        }

        LOG.debug("-> onRemove({})", this);
=======

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
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
    }

    @Override
    public void onUp(@NonNull final Node node) {
<<<<<<< HEAD
        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp({})", node);
        }
    }

    @Override
    public String toString() {
        return "CosmosLoadBalancingPolicy({"
            + "configuration:{"
            + Option.DNS_EXPIRY_TIME.getName() + '=' + this.dnsExpiryTimeInSeconds + ','
            + Option.GLOBAL_ENDPOINT.getName() + "=\"" + this.globalEndpoint + "\","
            + Option.READ_DATACENTER.getName() + "=\"" + this.readDatacenter + "\","
            + Option.WRITE_DATACENTER.getName() + "=\"" + this.writeDatacenter + "\""
            + "},datacenter-nodes:{"
            + "read-local" + "=" + toString(this.localNodesForReading) + ","
            + "remote" + "=" + toString(this.remoteNodes) + ","
            + "write-local" + "=" + toString(this.localNodesForWriting)
            + "}})";
=======

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
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
    }

    // endregion

    // region Privates

    /**
     * DNS lookup based on the {@link #globalEndpoint} address.
     * <p>
     * If {@link #dnsExpiryTimeInSeconds} has elapsed, the array of local addresses is also updated.
     *
<<<<<<< HEAD
     * @return value of {@link #localAddresses} which will have been updated, if {@link #dnsExpiryTimeInSeconds} has
     * elapsed.
     *
     * @throws IllegalStateException if the DNS could not resolve the {@link #globalEndpoint} and local addresses have
     *                               not yet been enumerated.
     */
    @NonNull
=======
     * @return value of {@link #localAddresses} which will have been updated, if {@link #dnsExpiryTimeInSeconds}
     * has elapsed.
     *
     * @throws IllegalStateException if the DNS could not resolve the {@link #globalEndpoint} and local addresses have
     * not yet been enumerated.
     */
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
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

<<<<<<< HEAD
    private static void addTo(final List<Node> nodes, final Node node) {

        final EndPoint endPoint = node.getEndPoint();

        for (final Node current : nodes) {
            if (endPoint.equals(current.getEndPoint())) {
                return;
            }
        }

        nodes.add(node);
    }

    private static void addTo(final Queue<Node> queryPlan, final List<Node> nodes, final int start) {
=======
    private static void addTo(final ArrayDeque<Node> queryPlan, final int start, final List<Node> nodes) {
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98

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

<<<<<<< HEAD
    private void reclassifyNodes(
        final List<Node> nodes, final List<Node> localNodes, final List<Node> remoteNodes,
        final InetAddress[] localAddresses) {

        for (final Node node : nodes) {

            final InetAddress address = this.getAddress(node);
            boolean isLocalAddress = false;

            for (final InetAddress localAddress : localAddresses) {
                if (address.equals(localAddress)) {
                    isLocalAddress = true;
                    break;
                }
            }

            if (isLocalAddress) {
                if (!localNodes.contains(node)) {
                    localNodes.add(node);
                }
            } else {
                if (!remoteNodes.contains(node)) {
                    remoteNodes.add(node);
                }
            }
        }
    }

    private void refreshNodesIfDnsExpired() {

        synchronized (this.lock) {

            if (this.globalEndpoint.isEmpty() || !this.dnsExpired()) {
                return;
            }

            final InetAddress[] localAddresses = this.getLocalAddresses();
            final List<Node> localNodes = new ArrayList<>();
            final List<Node> remoteNodes = new ArrayList<>();

            this.reclassifyNodes(this.localNodesForWriting, localNodes, remoteNodes, localAddresses);
            this.reclassifyNodes(this.remoteNodes, localNodes, remoteNodes, localAddresses);
            this.localNodesForWriting = localNodes;
            this.remoteNodes = remoteNodes;
        }
    }

    private static void removeFrom(final List<Node> nodes, final Node node) {
        final EndPoint endPoint = node.getEndPoint();
        int i = 0;
        for (final Node current : nodes) {
            if (endPoint.equals(current.getEndPoint())) {
                nodes.remove(i);
                break;
            }
            i++;
        }
    }

    private void reportDistance(@NonNull final Node node) {

        final String datacenter = node.getDatacenter();
        NodeDistance distance = NodeDistance.IGNORED;

        if (datacenter != null) {

            if (!this.readDatacenter.isEmpty() && Objects.equals(datacenter, this.readDatacenter)) {
                addTo(this.localNodesForReading, node);
                distance = NodeDistance.LOCAL;
            }

            if (!this.writeDatacenter.isEmpty() && Objects.equals(datacenter, this.writeDatacenter)) {
                addTo(this.localNodesForWriting, node);
                distance = NodeDistance.LOCAL;
            } else if (this.dnsLookupAddresses.contains(this.getAddress(node))) {
                addTo(this.localNodesForWriting, node);
                distance = NodeDistance.LOCAL;
            } else if (distance == NodeDistance.IGNORED) {
                addTo(this.remoteNodes, node);
                distance = NodeDistance.REMOTE;
            }
        }

        this.distanceReporter.setDistance(node, distance);
    }

    @NonNull
    private static String toString(@Nullable final Node node) {
        return node == null ? "null" : "Node("
            + "endPoint=" + node.getEndPoint() + ","
            + "datacenter=" + node.getDatacenter() + ","
            + "distance=" + node.getDistance() + ","
            + "hostId=" + node.getHostId() + ","
            + "hashCode=" + node.hashCode()
            + ")";
    }

    @NonNull
    private static String toString(@NonNull final Collection<Node> nodes) {
        return nodes.stream().map(CosmosLoadBalancingPolicy::toString).collect(Collectors.joining(",", "[", "]"));
    }

    private void validateConfiguration() {
=======
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

    private void validate() {
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98

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

<<<<<<< HEAD
    // endregion

    // region Types

=======
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
    enum Option implements DriverOption {

        DNS_EXPIRY_TIME("dns-expiry-time", (option, profile) ->
            profile.getInt(option, option.getDefaultValue()),
            60),

        GLOBAL_ENDPOINT("global-endpoint", (option, profile) -> {
            final String value = profile.getString(option, option.getDefaultValue());
            assert value != null;
            final int index = value.lastIndexOf(':');
            return index < 0 ? value : value.substring(0, index);
        }, ""),

        READ_DATACENTER("read-datacenter", (option, profile) ->
            profile.getString(option, option.getDefaultValue()),
            ""),

        WRITE_DATACENTER("write-datacenter", (option, profile) ->
            profile.getString(option, option.getDefaultValue()),
            "");

        private final Object defaultValue;
        private final BiFunction<Option, DriverExecutionProfile, ?> getter;
<<<<<<< HEAD
        private final String name;
=======
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
        private final String path;

        <T, R> Option(
            final String name, final BiFunction<Option, DriverExecutionProfile, R> getter, final T defaultValue) {

            this.defaultValue = defaultValue;
            this.getter = getter;
<<<<<<< HEAD
            this.name = name;
            this.path = OPTION_PATH_PREFIX + name;
=======
            this.path = PATH_PREFIX + name;
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
        }

        @SuppressWarnings("unchecked")
        public <T> T getDefaultValue() {
            return (T) this.defaultValue;
        }

        @NonNull
<<<<<<< HEAD
        public String getName() {
            return this.name;
        }

        @NonNull
=======
>>>>>>> af78f426eb7e96f31c5851b02bec5bb84de0da98
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
