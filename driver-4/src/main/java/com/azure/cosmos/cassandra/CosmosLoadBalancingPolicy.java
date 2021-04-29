// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.implementation.Json;
import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS;

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

    static {
        Json.module()
            .addSerializer(Node.class, NodeSerializer.INSTANCE)
            .addSerializer(Request.class, RequestSerializer.INSTANCE);
    }

    private final InternalDriverContext driverContext;
    private final MetadataManager metadataManager;
    private final boolean multiRegionWrites;
    private final Set<Node> nodesForReading;
    private final Set<Node> nodesForWriting;
    private final List<String> preferredRegions;
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

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);
        this.driverContext = (InternalDriverContext) driverContext;
        this.metadataManager = this.driverContext.getMetadataManager();

        this.multiRegionWrites = MULTI_REGION_WRITES.getValue(profile, Boolean.class);
        this.preferredRegions = PREFERRED_REGIONS.getValue(profile, List.class);

        this.nodesForReading = new ConcurrentSkipListSet<>(new PreferredRegionsComparator(
            this.metadataManager,
            this.preferredRegions));

        this.nodesForWriting = this.multiRegionWrites
            ? this.nodesForReading
            : new ConcurrentSkipListSet<>(new PreferredRegionsComparator(this.metadataManager, this.preferredRegions));
    }

    // endregion

    // region Accessors

    /**
     * Returns a value of {@code true}, if multi region writes are enabled.
     *
     * @return a value of {@code true}, if multi region writes are enabled; {@code false} otherwise.
     */
    public boolean getMultiRegionWrites() {
        return this.multiRegionWrites;
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
     * Gets the list of preferred regions for failover.
     *
     * @return The list of preferred regions for failover.
     */
    public List<String> getPreferredRegions() {
        return Collections.unmodifiableList(this.preferredRegions);
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
            LOG.debug("init({})", Json.toJson(nodes.values()));
        }

        this.distanceReporter = distanceReporter;

        for (final Node node : nodes.values()) {
            if (this.preferredRegions.contains(node.getDatacenter())) {
                distanceReporter.setDistance(node, NodeDistance.LOCAL);
            } else {
                distanceReporter.setDistance(node, NodeDistance.REMOTE);
            }
            this.nodesForReading.add(node);
        }

        if (!this.multiRegionWrites) {

            // If you're connected to a Cosmos DB Cassandra API instance, there should be a single contact point,
            // the global endpoint. This code guards against other arrangements with this expectation: All contact
            // points must write capable.

            for (final Node node : this.getContactPointsOrException()) {
                if (this.preferredRegions.contains(node.getDatacenter())) {
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

        this.metadataManager.refreshNodes().whenComplete((ignored, error) -> {

            if (error != null) {
                LOG.error("node refresh failed due to ", error);
            } else {
                if (LOG.isDebugEnabled()) {
                    final Map<UUID, Node> refreshedNodes = this.metadataManager.getMetadata().getNodes();
                    LOG.debug("refreshed nodes: {}", Json.toJson(refreshedNodes));
                }
            }

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
            LOG.debug("newQueryPlan(request: {}, session: {})",
                Json.toJson(request),
                Json.toJson(session == null ? null : session.getName()));
        }

        final Function<Request, Queue<Node>> function = this.getNodes;
        final Queue<Node> nodes = function.apply(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("newQueryPlan -> returns({}), {}", Json.toJson(nodes), this);
        }

        return nodes;
    }

    @Override
    public void onAdd(@NonNull final Node node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onAdd({})", Json.toJson(node));
        }

        if (this.preferredRegions.contains(node.getDatacenter())) {
            this.distanceReporter.setDistance(node, NodeDistance.LOCAL);
        } else {
            this.distanceReporter.setDistance(node, NodeDistance.REMOTE);
        }

        this.nodesForReading.add(node);

        if (!this.multiRegionWrites) {
            if (this.getContactPointsOrException().contains(node)) {
                this.nodesForWriting.add(node);
            }
        }

        LOG.debug("onAdd -> returns(void), {}", this);
    }

    @Override
    public void onDown(@NonNull final Node node) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onDown({})", Json.toJson(node));
        }
    }

    @Override
    public void onRemove(@NonNull final Node node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing node: onRemove({})", Json.toJson(node));
        }

        this.nodesForReading.remove(node);

        if (!this.multiRegionWrites) {
            this.nodesForWriting.remove(node);
        }

        LOG.debug("onRemove -> returns(void), {}", this);

        if (this.multiRegionWrites) {
            if (this.nodesForReading.isEmpty()) {
                LOG.warn("All nodes have now been removed: {}", this);
            }
        } else {
            if (this.nodesForReading.isEmpty()) {
                LOG.warn("All nodes for reading have now been removed: {}", this);
            }
            if (this.nodesForWriting.isEmpty()) {
                LOG.warn("All nodes for writing have now been removed: {}", this);
            }
        }
    }

    @Override
    public void onUp(@NonNull final Node node) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp({})", Json.toJson(node));
        }
    }

    @Override
    public String toString() {
        return Json.toString(this);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private Set<Node> getContactPoints() {
        return (Set<Node>) (Set<?>) this.driverContext.getMetadataManager().getContactPoints();
    }

    @NonNull
    private Set<Node> getContactPointsOrException() {
        return Objects.requireNonNull(this.getContactPoints(), "expected non-null contactPoints");
    }

    // endregion

    // region Privates

    private Queue<Node> doGetNodes(final Request request) {
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

    private static class NodeSerializer extends StdSerializer<Node> {

        public static final NodeSerializer INSTANCE = new NodeSerializer(Node.class);
        private static final long serialVersionUID = -4853942224745141238L;

        NodeSerializer() {
            this(null);
        }

        NodeSerializer(final Class<Node> type) {
            super(type);
        }

        @Override
        public void serialize(
            @NonNull final Node value,
            @NonNull final JsonGenerator generator,
            @NonNull final SerializerProvider serializerProvider) throws IOException {

            Objects.requireNonNull(value, "expected non-null value");
            Objects.requireNonNull(value, "expected non-null generator");
            Objects.requireNonNull(value, "expected non-null serializerProvider");

            generator.writeStartObject();
            generator.writeStringField("endPoint", value.getEndPoint().toString());
            generator.writeStringField("datacenter", value.getDatacenter());
            generator.writeStringField("distance", value.getDistance().toString());

            final UUID hostId = value.getHostId();

            if (hostId == null) {
                generator.writeNullField("hostId");
            } else {
                generator.writeStringField("hostId", hostId.toString());
            }

            generator.writeNumberField("openConnections", value.getOpenConnections());
            generator.writeStringField("state", value.getState().toString());
            generator.writeEndObject();
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class PreferredRegionsComparator implements Comparator<Node> {

        private final Map<String, Integer> indexes;
        private final MetadataManager metadataManager;

        PreferredRegionsComparator(
            @NonNull final MetadataManager metadataManager,
            @NonNull final List<String> preferredRegions) {

            this.metadataManager = Objects.requireNonNull(metadataManager, "expected non-null contactPoints");

            this.indexes = new HashMap<>(
                Objects.requireNonNull(preferredRegions, "expected non-null preferredRegions").size());

            int index = 0;

            for (final String region : preferredRegions) {
                this.indexes.put(region, index++);
            }
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
         */
        @SuppressFBWarnings("RC_REF_COMPARISON")
        @SuppressWarnings("NumberEquality")
        @Override
        public int compare(@NonNull final Node x, @NonNull final Node y) {

            Objects.requireNonNull(x, "expected non-null x");
            Objects.requireNonNull(y, "expected non-null y");

            if (x == y) {
                return 0;
            }

            final String xDatacenter = x.getDatacenter();
            final String yDatacenter = y.getDatacenter();

            Objects.requireNonNull(xDatacenter, "expected non-null x::datacenter");
            Objects.requireNonNull(yDatacenter, "expected non-null y::datacenter");

            final int compareDatacenterNames = xDatacenter.compareTo(yDatacenter);

            if (compareDatacenterNames != 0) {

                final Integer xIndex = this.indexes.get(xDatacenter);
                final Integer yIndex = this.indexes.get(yDatacenter);

                int result;

                if (xIndex != yIndex) {

                    if (xIndex == null) {
                        return -1;  // y is a preferred datacenter and x is not
                    }

                    if (yIndex == null) {
                        return 1;  // x is a preferred datacenter and y is not
                    }

                    result = Integer.compare(xIndex, yIndex);

                    if (result != 0) {
                        return result; // x and y are preferred datacenters and one has higher priority than the other
                    }
                }

                final boolean xIsContactPoint = this.getContactPoints().contains(x);
                final boolean yIsContactPoint = this.getContactPoints().contains(y);

                result = Boolean.compare(xIsContactPoint, yIsContactPoint);

                if (result != 0) {
                    return result;  // one of x or y are a contact point
                }

                if (xIndex == null) {  // x and y are both not in a preferred datacenter
                    return compareDatacenterNames;
                }
            }

            // We distinguish x and y by Host ID because they're in the same datacenter

            final UUID xHostId = x.getHostId();
            final UUID yHostId = y.getHostId();

            Objects.requireNonNull(xHostId, "expected non-null x::hostId");
            Objects.requireNonNull(yHostId, "expected non-null y::hostId");

            return Objects.compare(x.getHostId(), y.getHostId(), UUID::compareTo);
        }

        @SuppressWarnings("unchecked")
        @NonNull
        private Set<Node> getContactPoints() {
            return (Set<Node>) (Set<?>) this.metadataManager.getContactPoints();
        }
    }

    private static class RequestSerializer extends StdSerializer<Request> {

        public static final RequestSerializer INSTANCE = new RequestSerializer(Request.class);
        private static final long serialVersionUID = -6046496854932624633L;

        RequestSerializer() {
            this(null);
        }

        RequestSerializer(final Class<Request> type) {
            super(type);
        }

        @Override
        public void serialize(
            @NonNull final Request value,
            @NonNull final JsonGenerator generator,
            @NonNull final SerializerProvider serializerProvider) throws IOException {

            Objects.requireNonNull(value, "expected non-null value");
            Objects.requireNonNull(value, "expected non-null generator");
            Objects.requireNonNull(value, "expected non-null serializerProvider");

            generator.writeStartObject();

            String query;

            try {
                final Method getQuery = value.getClass().getMethod("getQuery");
                query = (String) getQuery.invoke(value);
            } catch (final NoSuchMethodException | InvocationTargetException | IllegalAccessException error) {
                query = value.getClass().toString();
            }

            if (query == null) {
                generator.writeNullField("query");
            } else {
                generator.writeStringField("query", query);
            }

            final CqlIdentifier keyspace = value.getKeyspace();

            if (keyspace == null) {
                generator.writeNullField("keyspace");
            } else {
                generator.writeStringField("keyspace", keyspace.asCql(true));
            }

            final Duration timeout = value.getTimeout();

            if (timeout == null) {
                generator.writeNullField("timeout");
            } else {
                generator.writeObjectField("timeout", timeout);
            }

            generator.writeEndObject();
        }
    }

    // endregion
}
