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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.azure.cosmos.cassandra.CosmosJson.toJson;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS;
import static java.util.Objects.requireNonNull;

/**
 * A {@link LoadBalancingPolicy} implementation with an option to specify read and write datacenters to route requests.
 * <p>
 * This is the default load-balancing policy when you take a dependency on this package. It provides a good out-of-box
 * experience for communicating with Cosmos Cassandra API instances. Its behavior is specified in configuration:
 * <pre>{@code
 * datastax-java-driver.basic.load-balancing-policy {
 *   class = com.azure.cosmos.cassandra.CosmosLoadBalancingPolicy
 *   preferred-regions = [<datacenter-name>[,...]]
 *   multi-region-writes = [true|false]
 * }
 * }</pre>
 * If a list of {@code preferred-regions} is specified, {@linkplain Node nodes} in those regions are prioritized for
 * read requests. If {@code multi-region-writes} is {@code true}, this list is used to prioritize write requests as
 * well. Otherwise, if {@code multi-region-writes} is {@code false}, all write requests will be routed to the primary
 * region. This is assumed to be the address you specify with {@code datastax-java-driver.basic.contact-points}.
 * <p>
 *
 * @see <a href="../../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">DataStax Java
 * Driver Load balancing</a>
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    // region Fields

    private static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicy.class);

    private final InternalDriverContext driverContext;
    private final boolean multiRegionWritesEnabled;
    private final NavigableSet<Node> nodesForReading;
    private final NavigableSet<Node> nodesForWriting;
    private DistanceReporter distanceReporter;
    private volatile Function<Request, Queue<Node>> getNodes;

    // endregion

    // region Constructors

    /**
     * Initializes a newly created {@link CosmosLoadBalancingPolicy} object from configuration.
     *
     * @param driverContext an object holding the context of the current driver instance.
     * @param profileName   name of the configuration profile to apply.
     */
    @SuppressWarnings({ "unchecked" })
    public CosmosLoadBalancingPolicy(final DriverContext driverContext, final String profileName) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("CosmosLoadBalancingPolicy(driverContext: {}, profileName: {})",
                toJson(driverContext),
                toJson(profileName));
        }

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);
        this.driverContext = (InternalDriverContext) driverContext;

        this.multiRegionWritesEnabled = MULTI_REGION_WRITES.getValue(profile, Boolean.class);
        final List<String> preferredRegions = PREFERRED_REGIONS.getValue(profile, List.class);

        this.nodesForReading = new ConcurrentSkipListSet<>(new PreferredRegionsComparator(preferredRegions));

        this.nodesForWriting = this.multiRegionWritesEnabled
            ? this.nodesForReading
            : new ConcurrentSkipListSet<>(new PreferredRegionsComparator(Collections.emptyList()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("CosmosLoadBalancingPolicy -> {}", toJson(this));
        }
    }

    // endregion

    // region Accessors

    /**
     * Returns a value of {@code true}, if multi region writes are enabled.
     *
     * @return a value of {@code true}, if multi region writes are enabled; {@code false} otherwise.
     */
    public boolean getMultiRegionWritesEnabled() {
        return this.multiRegionWritesEnabled;
    }

    /**
     * Returns a copy of the current list of nodes for reading.
     * <p>
     * The nodes are sorted in priority order as determined by the preferred regions list. When multi-region writes are
     * enabled this list will be exactly the same as the list of nodes for writing. A node representing the global
     * endpoint will appear in the list. It will be the last node, if the primary region is absent from the list of
     * preferred regions.
     * <p>
     * In the rare case of a regional outage or transient loss of connectivity, this list can be empty. This is
     * extremely unlikely when your data is globally distributed.
     *
     * @return A copy of the current list of nodes for reading.
     */
    @SuppressWarnings("Java9CollectionFactory")
    public List<Node> getNodesForReading() {
        return Collections.unmodifiableList(new ArrayList<>(this.nodesForReading));
    }

    /**
     * Returns a copy of the current list of nodes for writing.
     * <p>
     * The nodes are sorted in priority order as determined by the preferred regions list. When multi-region writes are
     * enabled this list will be exactly the same as the list of nodes for reading. When multi-region writes are
     * disabled this list will contain a single node representing the global endpoint. In the rare case of a regional
     * outage or transient loss of connectivity, this list can be empty. This is extremely unlikely when your data is
     * globally distributed and multi-region writes are enabled.
     *
     * @return A copy of the current list of nodes for writing.
     */
    @SuppressWarnings("Java9CollectionFactory")
    public List<Node> getNodesForWriting() {
        return Collections.unmodifiableList(new ArrayList<>(this.nodesForWriting));
    }

    /**
     * Gets the list of preferred regions for failover on read operations.
     * <p>
     * When multi-region writes are enabled, this list will be the same as the one returned by {@link
     * #getPreferredWriteRegions}.
     *
     * @return The list of preferred regions for failover on read operations.
     */
    public List<String> getPreferredReadRegions() {
        final PreferredRegionsComparator comparator = (PreferredRegionsComparator) this.nodesForReading.comparator();
        assert comparator != null;
        return Collections.unmodifiableList(comparator.getPreferredRegions());
    }

    /**
     * Gets the list of preferred regions for failover on read operations.
     * <p>
     * When multi-region writes are enabled, this list will be the same as the one returned by {@link
     * #getPreferredReadRegions}.
     *
     * @return The list of preferred regions for failover on write operations.
     */
    public List<String> getPreferredWriteRegions() {
        final PreferredRegionsComparator comparator = (PreferredRegionsComparator) this.nodesForReading.comparator();
        assert comparator != null;
        return Collections.unmodifiableList(comparator.getPreferredRegions());
    }

    // endregion

    // region Methods

    /**
     * Closes the current {@link CosmosLoadBalancingPolicy} instance.
     * <p>
     * This method performs no action. It is a noop.
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
            LOG.debug("init({})", toJson(nodes.values()));
        }

        final MetadataManager metadataManager = this.driverContext.getMetadataManager();
        final Set<Node> contactPoints = this.getContactPointsOrThrow();

        PreferredRegionsComparator comparator = (PreferredRegionsComparator) this.nodesForReading.comparator();
        assert comparator != null;
        comparator.addPreferredRegions(contactPoints);

        this.distanceReporter = distanceReporter;

        for (final Node node : nodes.values()) {
            if (comparator.hasPreferredRegion(node.getDatacenter())) {
                distanceReporter.setDistance(node, NodeDistance.LOCAL);
            } else {
                distanceReporter.setDistance(node, NodeDistance.REMOTE);
            }
            this.nodesForReading.add(node);  // When multiRegionWrites is true, nodesForReading == nodesForWriting
        }

        if (this.multiRegionWritesEnabled) {
            assert this.nodesForReading == this.nodesForWriting;
        } else {

            // Here we assume that all contact points are write capable. If you're connected to a Cosmos DB Cassandra
            // API instance, there should be a single contact point, the global endpoint. If you're connected to an
            // Apache Cassandra instance, we assume that the contact points are in the datacenters to which you wish
            // to write.

            comparator = (PreferredRegionsComparator) this.nodesForWriting.comparator();
            assert comparator != null;
            comparator.addPreferredRegions(contactPoints);

            for (final Node node : contactPoints) {
                if (comparator.hasPreferredRegion(node.getDatacenter())) {
                    distanceReporter.setDistance(node, NodeDistance.LOCAL);
                } else {
                    distanceReporter.setDistance(node, NodeDistance.REMOTE);
                }
                assert nodes.containsKey(node.getHostId());
                this.nodesForWriting.add(node);
            }
        }

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
            } else {
                if (LOG.isDebugEnabled()) {
                    final Map<UUID, Node> refreshedNodes = metadataManager.getMetadata().getNodes();
                    LOG.debug("refreshed nodes: {}", toJson(refreshedNodes));
                }
            }

            permissionToGetNodes.release(Integer.MAX_VALUE);
            this.getNodes = this::doGetNodes;
        });

        LOG.debug("init -> {}", this);
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
            LOG.debug("newQueryPlan(request: {}, session: {})",
                toJson(request),
                toJson(session == null ? null : session.getName()));
        }

        // TODO (DANOBLE) consider caching results so that evaluation is reduced

        final Function<Request, Queue<Node>> function = this.getNodes;
        final Queue<Node> nodes = function.apply(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("newQueryPlan -> returns({}) from {}", toJson(nodes), this);
        }

        return nodes;
    }

    @Override
    public void onAdd(@NonNull final Node node) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onAdd({})", toJson(node));
        }
        this.onUp(node);
    }

    @Override
    public void onDown(@NonNull final Node node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onDown({})", toJson(node));
        }

        requireNonNull(node, "expected non-null node");
        this.nodesForReading.remove(node);

        if (this.multiRegionWritesEnabled) {
            assert this.nodesForReading == this.nodesForWriting;
        } else {
            this.nodesForWriting.remove(node);
        }

        if (LOG.isWarnEnabled()) {
            if (this.multiRegionWritesEnabled) {
                if (this.nodesForReading.isEmpty()) {
                    LOG.warn("All nodes have now been removed: {}", toJson(this));
                }
            } else {
                if (this.nodesForReading.isEmpty()) {
                    LOG.warn("All nodes for reading have now been removed: {}", toJson(this));
                }
                if (this.nodesForWriting.isEmpty()) {
                    LOG.warn("All nodes for writing have now been removed: {}", toJson(this));
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("onDown -> {}", this);
        }
    }

    @Override
    public void onRemove(@NonNull final Node node) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onRemove({})", toJson(node));
        }
        this.onDown(node);
    }

    @Override
    public void onUp(@NonNull final Node node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp({})", toJson(node));
        }

        PreferredRegionsComparator comparator = (PreferredRegionsComparator) this.nodesForReading.comparator();
        assert comparator != null;

        this.distanceReporter.setDistance(node, comparator.hasPreferredRegion(node.getDatacenter())
            ? NodeDistance.LOCAL
            : NodeDistance.REMOTE);

        this.nodesForReading.add(node);

        if (this.multiRegionWritesEnabled) {
            assert this.nodesForReading == this.nodesForWriting;
        } else {
            comparator = (PreferredRegionsComparator) this.nodesForWriting.comparator();
            assert comparator != null;
            if (comparator.hasPreferredRegion(node.getDatacenter())) {
                this.nodesForWriting.add(node);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp -> {}", this);
        }
    }

    @Override
    public String toString() {
        return CosmosJson.toString(this);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private Set<Node> getContactPoints() {
        return (Set<Node>) (Set<?>) this.driverContext.getMetadataManager().getContactPoints();
    }

    @NonNull
    private Set<Node> getContactPointsOrThrow() {
        return requireNonNull(this.getContactPoints(), "expected non-null contactPoints");
    }

    // endregion

    // region Privates

    private Queue<Node> doGetNodes(final Request request) {
        // TODO (DANOBLE) consider eliminating copy
        return new ConcurrentLinkedQueue<>(isReadRequest(request) ? this.nodesForReading : this.nodesForWriting);
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

    // endregion

    // region Types

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static final class PreferredRegionsComparator implements Comparator<Node> {

        private final Map<String, Integer> indexes;
        private List<String> preferredRegions;

        PreferredRegionsComparator(@NonNull final List<String> preferredRegions) {

            requireNonNull(preferredRegions, "expected non-null preferredRegions");

            this.indexes = new HashMap<>(preferredRegions.size() + 1);
            int index = 0;

            for (final String region : preferredRegions) {
                this.indexes.put(region, index++);
            }

            this.preferredRegions = null;
        }

        /**
         * Compares two {@linkplain Node nodes} based on an ordering that ensures nodes in preferred regions followed by
         * nodes that were specified as contact points sort before nodes that are neither.
         * <p>
         * Nodes that don't belong to datacenters in the preferred region list are compared alphabetically by datacenter
         * name, then by host ID. This results in predictable failover routing behavior. If all preferred regions are
         * down and no contact points are available, other datacenters will be considered in alphabetic order.
         *
         * @param x One {@linkplain Node node}.
         * @param y Another {@linkplain Node node}.
         *
         * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or
         * greater than the second.
         *
         * @throws NullPointerException if x or y have no datacenter name or host ID.
         */
        @SuppressFBWarnings(value = "RC_REF_COMPARISON", justification = "Reference comparison is intentional")
        @SuppressWarnings("NumberEquality")
        @Override
        public int compare(@NonNull final Node x, @NonNull final Node y) {

            if (x == y) {
                return 0;
            }

            final String xDatacenter = requireNonNull(x.getDatacenter(), "expected non-null x::datacenter");
            final String yDatacenter = requireNonNull(y.getDatacenter(), "expected non-null y::datacenter");

            final int xIndex = this.indexes.getOrDefault(xDatacenter, this.preferredRegions.size());
            final int yIndex = this.indexes.getOrDefault(yDatacenter, this.preferredRegions.size());

            int result = Integer.compare(xIndex, yIndex);

            if (result != 0) {
                return result;  // x and y are in different regions and one sorts before the other
            }

            // This remainder of this method covers Apache Cassandra use cases with some grace

            // We first distinguish x and y by datacenter name, lexicographically

            result = xDatacenter.compareTo(yDatacenter);

            if (result != 0) {
                return result;
            }

            // We then distinguish x and y by Host ID they're in the same datacenter

            final UUID xHostId = requireNonNull(x.getHostId(), "expected non-null x::hostId");
            final UUID yHostId = requireNonNull(y.getHostId(), "expected non-null y::hostId");

            return xHostId.compareTo(yHostId);
        }

        /**
         * Called by {@link #init} to add the datacenters for all contact points to the list of preferred regions.
         *
         * These regions will appear last in the list of preferred regions following those specified in configuration
         * using {@link CosmosLoadBalancingPolicyOption#PREFERRED_REGIONS}.
         * <p>
         * This method is not thread safe and can only be called once. It should only be called by {@link #init}.
         *
         * @param contactPoints A set of contact points.
         *
         * @throws IllegalStateException if this method is called more than once.
         */
        void addPreferredRegions(final Set<Node> contactPoints) {
            if (this.preferredRegions != null) {
                throw new IllegalStateException("attempt to add preferred regions more than once");
            }
            contactPoints.stream()
                .map(Node::getDatacenter)
                .forEachOrdered(region -> this.indexes.put(region, this.indexes.size()));
            this.preferredRegions = this.collectPreferredRegions();
        }

        boolean hasPreferredRegion(final String name) {
            return this.indexes.containsKey(name);
        }

        List<String> getPreferredRegions() {
            return this.preferredRegions == null ? this.collectPreferredRegions() : this.preferredRegions;
        }

        private List<String> collectPreferredRegions() {
            return this.indexes.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        }
    }

    // endregion
}
