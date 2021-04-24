// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.DNS_EXPIRY_TIME;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.READ_DATACENTER;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.WRITE_DATACENTER;

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
 * requests. To determine the datacenter for write requests, a value for either {@code global-endpoint} or {@code
 * write-datacenter} must then be provided. If {@code write-datacenter} is specified, writes will be prioritized for
 * that location. When a {@code global-endpoint} is specified, write requests will be prioritized for the default write
 * location.
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

    private static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicy.class);

    private final int dnsExpiryTimeInSeconds;
    private final List<InetAddress> dnsLookupAddresses;
    private final InternalDriverContext driverContext;
    private final AtomicReference<String> globalEndpoint;
    private final AtomicInteger index;
    private final Object lock;
    private final String readDatacenter;
    private final String writeDatacenter;

    private DistanceReporter distanceReporter;

    private volatile Function<Request, Queue<Node>> getNodes;
    private volatile long lastDnsLookupTime;

    @SuppressFBWarnings("VO_VOLATILE_REFERENCE_TO_ARRAY")
    private volatile InetAddress[] localAddresses;

    private volatile List<Node> localNodesForReading;
    private volatile List<Node> localNodesForWriting;
    private volatile List<Node> remoteNodes;

    // endregion

    // region Constructors

    /**
     * Initializes a newly created {@link CosmosLoadBalancingPolicy} object from configuration.
     *
     * @param driverContext an object holding the context of the current driver instance.
     * @param profileName   name of the configuration profile to apply.
     */
    @SuppressWarnings("Java9CollectionFactory")
    public CosmosLoadBalancingPolicy(final DriverContext driverContext, final String profileName) {

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.dnsExpiryTimeInSeconds = DNS_EXPIRY_TIME.getValue(profile, Integer.class);
        this.globalEndpoint = new AtomicReference<>(GLOBAL_ENDPOINT.getValue(profile, String.class));
        this.readDatacenter = READ_DATACENTER.getValue(profile, String.class);
        this.writeDatacenter = WRITE_DATACENTER.getValue(profile, String.class);

        this.driverContext = (InternalDriverContext) driverContext;
        this.index = new AtomicInteger();
        this.lastDnsLookupTime = Long.MAX_VALUE;
        this.localAddresses = null;
        this.lock = new Object();

        this.dnsLookupAddresses = this.getGlobalEndpoint().isEmpty()
            ? Collections.emptyList()
            : Collections.unmodifiableList(Arrays.asList(this.getLocalAddresses()));
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

        return this.globalEndpoint.updateAndGet(globalEndpoint -> {

            if (globalEndpoint.isEmpty() && this.writeDatacenter.isEmpty()) {

                final Set<DefaultNode> contactPoints = this.driverContext.getMetadataManager().getContactPoints();

                if (contactPoints != null && contactPoints.size() > 0) {
                    for (final DefaultNode contactPoint : contactPoints) {
                        final InetSocketAddress address = (InetSocketAddress) contactPoint.getEndPoint().resolve();
                        return address.getHostName();
                    }
                }
            }

            return globalEndpoint;
        });
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
     * @param nodes            the nodes to be examined.
     * @param distanceReporter an object that the policy uses to signal decisions it makes about node distances.
     */
    @Override
    public void init(@NonNull final Map<UUID, Node> nodes, @NonNull final DistanceReporter distanceReporter) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("init({})", toString(nodes.values()));
        }

        this.distanceReporter = distanceReporter;
        this.localNodesForReading = new ArrayList<>();
        this.remoteNodes = new ArrayList<>();
        this.localNodesForWriting = new ArrayList<>();

        for (final Node node : nodes.values()) {
            this.reportDistanceAndClassify(node);
        }

        final MetadataManager metadataManager = this.driverContext.getMetadataManager();
        final Semaphore permissionToGetNodes = new Semaphore(Integer.MAX_VALUE);

        this.getNodes = request -> {
            permissionToGetNodes.acquireUninterruptibly();
            try {
                return this.doGetNodes(request);
            } finally {
                permissionToGetNodes.release();
            }
        };

        permissionToGetNodes.acquireUninterruptibly(Integer.MAX_VALUE);

        metadataManager.refreshNodes().whenComplete((ignored, error) -> {

            if (error != null) {
                LOG.error("node refresh failed due to ", error);
            }

            final int size = this.driverContext.getMetadataManager().getMetadata().getNodes().size();
            this.index.set(new Random().nextInt(Math.max(size, 1)));
            permissionToGetNodes.release(Integer.MAX_VALUE);
            this.getNodes = this::doGetNodes;
        });

        LOG.debug("init -> returns(void), {}", this);
    }

    /**
     * Returns an {@link Queue ordered list} of {@link Node coordinators} to use for a new query.
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
     */
    @Override
    @NonNull
    public Queue<Node> newQueryPlan(@Nullable final Request request, @Nullable final Session session) {

        if (LOG.isDebugEnabled()) {
            if (request == null) {
                LOG.debug("newQueryPlan({\"request\":null,\"session\":{}})",
                    session == null ? "null" : '"' + session.getName() + '"');
            } else {
                LOG.debug("newQueryPlan({\"request\":{\"node\":{},\"keyspace\":\"{}\"},\"session\":{}})",
                    toString(request.getNode()),
                    request.getKeyspace(),
                    session == null ? "null" : '"' + session.getName() + '"');
            }
        }

        final Function<Request, Queue<Node>> function = this.getNodes;
        final Queue<Node> nodes = function.apply(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("newQueryPlan -> returns({}), {}", toString(nodes), this);
        }

        return nodes;
    }

    @Override
    public void onAdd(@NonNull final Node node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onAdd({})", toString(node));
        }

        synchronized (this.lock) {
            this.reportDistanceAndClassify(node);
        }

        LOG.debug("onAdd -> returns(void), {}", this);
    }

    @Override
    public void onDown(@NonNull final Node node) {
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

        LOG.debug("onRemove -> returns(void), {}", this);
    }

    @Override
    public void onUp(@NonNull final Node node) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp({})", toString(node));
        }
    }

    @Override
    public String toString() {
        return "CosmosLoadBalancingPolicy({"
            + "\"configuration\":{"
            + '"' + DNS_EXPIRY_TIME.getName() + "\":" + this.dnsExpiryTimeInSeconds + ','
            + '"' + GLOBAL_ENDPOINT.getName() + "\":\"" + this.globalEndpoint + "\","
            + '"' + READ_DATACENTER.getName() + "\":\"" + this.readDatacenter + "\","
            + '"' + WRITE_DATACENTER.getName() + "\":\"" + this.writeDatacenter + "\""
            + "},\"datacenter-nodes\":{"
            + "\"read-local\":" + toString(this.localNodesForReading) + ","
            + "\"remote\":" + toString(this.remoteNodes) + ","
            + "\"write-local\":" + toString(this.localNodesForWriting)
            + "}})";
    }

    /**
     * DNS lookup based on the {@link #globalEndpoint} address.
     * <p>
     * If {@link #dnsExpiryTimeInSeconds} has elapsed, the array of local addresses is also updated.
     *
     * @return value of {@link #localAddresses} which will have been updated, if {@link #dnsExpiryTimeInSeconds} has
     * elapsed.
     *
     * @throws IllegalStateException if the DNS could not resolve the {@link #globalEndpoint} and local addresses have
     *                               not yet been enumerated.
     */
    @NonNull
    private InetAddress[] getLocalAddresses() {

        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.getGlobalEndpoint());
                this.lastDnsLookupTime = System.currentTimeMillis() / 1000;
            } catch (final UnknownHostException error) {
                // DNS entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalStateException("The DNS could not resolve "
                        + GLOBAL_ENDPOINT.getPath()
                        + " = "
                        + this.globalEndpoint
                        + " the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    // endregion

    // region Privates

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

        final int length = nodes.size();
        final int end = start + length;

        for (int i = start; i < end; i++) {
            queryPlan.add(nodes.get(i % length));
        }
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis() / 1000 > this.lastDnsLookupTime + this.dnsExpiryTimeInSeconds;
    }

    private Queue<Node> doGetNodes(final Request request) {

        this.refreshNodesIfDnsExpired();

        final int start = this.index.updateAndGet(value -> value > Integer.MAX_VALUE - 10_000 ? 0 : value + 1);
        final ConcurrentLinkedQueue<Node> queryPlan = new ConcurrentLinkedQueue<>();
        final boolean readRequest = isReadRequest(request);

        if (readRequest) {
            addTo(queryPlan, this.localNodesForReading, start);
        }

        addTo(queryPlan, this.localNodesForWriting, start);
        addTo(queryPlan, this.remoteNodes, start);

        return queryPlan;
    }

    private InetAddress getAddress(final Node node) {

        final SocketAddress socketAddress = node.getEndPoint().resolve();

        if (!(socketAddress instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("expected node with InetSocketAddress, not " + socketAddress.getClass());
        }

        final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;

        if (inetSocketAddress.isUnresolved()) {

            // No guarantee that the datastax-java-driver has resolved the host name before this method is called

            final String hostName = inetSocketAddress.getHostName();

            try {
                return InetAddress.getByName(hostName);
            } catch (final UnknownHostException error) {
                throw new IllegalArgumentException("cannot determine InetAddress of node: " + toString(node), error);
            }
        }

        return inetSocketAddress.getAddress();
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

    private void reclassifyNodes(
        @NonNull final List<Node> nodes,
        @NonNull final List<Node> localNodes,
        @NonNull final List<Node> remoteNodes,
        @NonNull final InetAddress[] localAddresses) {

        Objects.requireNonNull(nodes, "expected non-null nodes");
        Objects.requireNonNull(localNodes, "expected non-null localNodes");
        Objects.requireNonNull(remoteNodes, "expected non-null remoteNodes");
        Objects.requireNonNull(localAddresses, "expected non-null localAddresses");

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

        final String globalEndpoint = this.getGlobalEndpoint();

        if (globalEndpoint == null || globalEndpoint.isEmpty()) {
            return;
        }

        synchronized (this.lock) {

            if (!this.dnsExpired()) {
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

    private void reportDistanceAndClassify(@NonNull final Node node) {

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
            } else {
                if (distance == NodeDistance.IGNORED) {
                    distance = NodeDistance.REMOTE;
                }
                addTo(this.remoteNodes, node);
            }
        }

        LOG.debug("reportDistanceAndClassify({}) -> setting distance to {}", toString(node), distance);
        this.distanceReporter.setDistance(node, distance);
    }

    @NonNull
    private static String toString(@Nullable final Object that) {

        final Node node = (Node) that;

        return node == null ? "null" : "{"
            + "\"endPoint\":\"" + node.getEndPoint() + "\","
            + "\"datacenter\":\"" + node.getDatacenter() + "\","
            + "\"distance\":\"" + node.getDistance() + "\","
            + "\"hostId\":\"" + node.getHostId() + "\","
            + "\"hashCode\":\"" + node.hashCode() + "\""
            + "}";
    }

    @NonNull
    private static String toString(@Nullable final Collection<? super Node> nodes) {
        return nodes != null
            ? nodes.stream().map(CosmosLoadBalancingPolicy::toString).collect(Collectors.joining(",", "[", "]"))
            : "null";
    }

//    private void validateConfiguration() {
//
//        final String globalEndpoint = this.getGlobalEndpoint();
//
//        if (globalEndpoint == null || globalEndpoint.isEmpty()) {
//
//            if (this.writeDatacenter.isEmpty() || this.readDatacenter.isEmpty()) {
//                throw new IllegalArgumentException("When " + GLOBAL_ENDPOINT.getPath() + " is not specified, "
//                    + "you must specify both " + READ_DATACENTER.getPath() + " and "
//                    + WRITE_DATACENTER.getPath() + ".");
//            }
//
//        } else if (!this.writeDatacenter.isEmpty()) {
//            throw new IllegalArgumentException("When " + GLOBAL_ENDPOINT.getPath() + " is specified, you must "
//                + "not specify " + WRITE_DATACENTER.getPath() + ". Writes will go to the default write region "
//                + "when " + GLOBAL_ENDPOINT.getPath() + " is specified.");
//        }
//    }

    // endregion
}
