// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.implementation.Json;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static java.util.Objects.requireNonNull;

/**
 * Implements a {@link LoadBalancingPolicy} with an option to specify read datacenter and write datacenter to route read
 * and write requests to their corresponding data centers.
 * <p>
 * If a {@code readDC} is specified, we prioritize nodes in the read datacenter for read requests. Either one of {@code
 * writeDC} or {@code globalEndpoint} must be specified to determine where write requests are routed. If {@code writeDC}
 * is specified, writes will be prioritized for that region. When {@code globalEndpoint} is specified, write requests
 * will be prioritized for the default write region. The {@code globalEndpoint} allows the client to gracefully fail
 * over by updating the default write region addresses. In this case {@code dnsExpirationInSeconds} specifies the time
 * to take to recover from the failover. By default, it is {@code 60} seconds.
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    // region Fields

    private static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicy.class);
    private static final Method GET_CONTACT_POINTS;

    private final boolean multiRegionWritesEnabled;
    private final List<String> preferredRegions;

    private List<Host> contactPoints;
    private Set<Host> hostsForReading;
    private Set<Host> hostsForWriting;

    static {
        try {
            GET_CONTACT_POINTS = Metadata.class.getDeclaredMethod("getContactPoints");
            AccessController.doPrivileged(new PrivilegedAction<Method>() {
                public Method run() {
                    try {
                        GET_CONTACT_POINTS.setAccessible(true);
                        return GET_CONTACT_POINTS;
                    } catch (final Throwable error) {
                        LOG.error("Privileged action on {} to setAccessible(true) failed due to: ",
                            GET_CONTACT_POINTS,
                            error);
                        throw error;
                    }
                }
            });

        } catch (final Throwable error) {
            LOG.error("Class initialization failed due to: ", error);
            throw new ExceptionInInitializerError(error);
        }
    }

    // endregion

    // region Constructors

    private CosmosLoadBalancingPolicy(
        @NonNull final List<String> preferredRegions,
        final boolean multiRegionWritesEnabled) {

        this.multiRegionWritesEnabled = multiRegionWritesEnabled;
        this.preferredRegions = new ArrayList<>(preferredRegions);
    }

    /**
     * Initializes a newly constructed {@link CosmosLoadBalancingPolicy} instance with default settings.
     * <p>
     * Multi-region writes are disabled and all requests are sent to the global endpoint.
     */
    public CosmosLoadBalancingPolicy() {
        this(Collections.emptyList(), false);
    }

    // endregion

    // region Accessors

    /**
     * Returns the list of contact points for the cluster with this {@link CosmosLoadBalancingPolicy}.
     *
     * @return the list of contact points for the cluster with this {@link CosmosLoadBalancingPolicy}.
     */
    public List<Host> getContactPoints() {
        return Collections.unmodifiableList(new ArrayList<>(this.contactPoints));
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
    public List<Host> getHostsForReading() {
        return Collections.unmodifiableList(new ArrayList<>(this.hostsForReading));
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
    public List<Host> getHostsForWriting() {
        return Collections.unmodifiableList(new ArrayList<>(this.hostsForWriting));
    }

    /**
     * Returns a value of {@code true}, if multi region writes are enabled.
     *
     * @return a value of {@code true}, if multi region writes are enabled; {@code false} otherwise.
     */
    public boolean getMultiRegionWritesEnabled() {
        return this.multiRegionWritesEnabled;
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
     * Gets a newly created {@link CosmosLoadBalancingPolicy.Builder builder} object for constructing a {@link
     * CosmosLoadBalancingPolicy}.
     *
     * @return a newly created {@link CosmosLoadBalancingPolicy} builder instance.
     */
    @NonNull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Closes the current {@link CosmosLoadBalancingPolicy} object.
     */
    @Override
    public void close() {
        // nothing to do
    }

    /**
     * Gets a newly created {@link CosmosLoadBalancingPolicy} object with default settings.
     *
     * @return a newly created {@link CosmosLoadBalancingPolicy} object with default settings.
     */
    @NonNull
    public static CosmosLoadBalancingPolicy defaultPolicy() {
        return new CosmosLoadBalancingPolicy();
    }

    /**
     * Return the {@link HostDistance} of the provided {@link Host}.
     *
     * @param host the host of which to return the distance of.
     *
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(final Host host) {

        // TODO (DANOBLE) Does the driver ever act based on distance or does it simply inform LoadBalancingPolicy?
        //  Does it matter that we say that all but the first preferred region is at HostDistance.REMOTE?

        final String datacenter = host.getDatacenter();

        if (datacenter == null) {
            LOG.warn("{} Returning HostDistance.IGNORED because datacenter is unknown", host);
            return HostDistance.IGNORED;
        }

        return datacenter.equals(this.preferredRegions.get(0)) ? HostDistance.LOCAL : HostDistance.REMOTE;
    }

    /**
     * Initializes the list of hosts in read, write, local, and remote categories.
     *
     * @throws NullPointerException if the contact points for {@code cluster} cannot be obtained.
     */
    @Override
    public void init(@NonNull final Cluster cluster, @NonNull final Collection<Host> hosts) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("init({})", toJson(hosts));
        }

        requireNonNull(cluster, "expected non-null cluster");
        requireNonNull(hosts, "expected non-null hosts");

        // Finalize the list of preferred regions

        this.contactPoints = getContactPointsOrThrow(cluster);

        this.contactPoints.stream()
            .map(host -> {
                final String region = host.getDatacenter();
                if (region == null) {
                    LOG.warn("Datacenter for contact point is unknown: {}", toJson(host));
                }
                return region;
            })
            .filter(Objects::nonNull)
            .distinct()
            .forEachOrdered(region -> {
                if (!this.preferredRegions.contains(region)) {
                    this.preferredRegions.add(region);
                }
            });

        // Initialize the hosts for read and write requests

        this.hostsForReading = new ConcurrentSkipListSet<>(new PreferredRegionsComparator(this.preferredRegions));

        this.hostsForReading.addAll(hosts.stream()
            .filter(host -> {
                requireNonNull(host, "expected non-null host");
                final String datacenter = host.getDatacenter();
                if (datacenter == null) {
                    LOG.warn("Datacenter for host is unknown: {}", host);
                    return false;
                }
                return true;
            })
            .collect(Collectors.toList()));

        if (this.multiRegionWritesEnabled) {
            this.hostsForWriting = this.hostsForReading;
        } else {

            // Here we assume that all contact points are write capable. If you're connected to a Cosmos DB Cassandra
            // API instance, there should be a single contact point, the global endpoint. This likely won't be the case
            // if you're connected to an Apache Cassandra instance. Since this CosmosLoadBalancingPolicy is configured
            // with multi-region writes disabled, we assume that the contact points are in the datacenters to which
            // write requests should be sent.

            this.hostsForWriting = new ConcurrentSkipListSet<>(new PreferredRegionsComparator(this.preferredRegions));

            for (final Host host : hosts) {
                if (this.contactPoints.contains(host)) {
                    this.hostsForWriting.add(host);
                }
            }
        }

        // TODO (DANOBLE) consider brining in the semaphore code to increase the probability we've got a full list of
        //  nodes before we hit CosmosLoadBalancingPolicy::newQueryPlan for the first time.
        //  See: driver-4/src/main/java/com/azure/cosmos/cassandra/CosmosLoadBalancingPolicy.java

        if (LOG.isDebugEnabled()) {
            LOG.debug("init -> {}", this);
        }
    }

    /**
     * Returns the hosts to use for a new query.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement      the query for which to build the plan.
     *
     * @return An iterator over the hosts to be queried in the order in which they are to be tried. The iterator is
     * subject to changes in the underlying host set, but will not throw a
     * {@link java.util.ConcurrentModificationException}.
     */
    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("newQueryPlan(loggedKeyspace: {}, statement: {})",
                toJson(loggedKeyspace),
                toJson(statement));
        }

        final Set<Host> hosts = isReadRequest(statement) ? this.hostsForReading : this.hostsForWriting;

        if (LOG.isDebugEnabled()) {
            LOG.debug("newQueryPlan -> returns({}) from {}", hosts.iterator(), this);
        }

        return hosts.iterator();
    }

    @Override
    public void onAdd(final Host host) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onAdd({})", toJson(host));
        }
        this.onUp(host);
    }

    @Override
    public void onDown(final Host host) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onDown({})", toJson(host));
        }

        requireNonNull(host, "expected non-null host");

        if (host.getDatacenter() == null) {

            if (this.hostsForReading.stream().anyMatch(host::equals)) {
                throw new IllegalStateException(
                    "Host without a datacenter should not be in list of hosts for reading: "
                        + Json.toString(host));
            }

            if (!this.multiRegionWritesEnabled) {

                assert this.hostsForReading == this.hostsForWriting;

                if (this.hostsForWriting.stream().anyMatch(host::equals)) {
                    throw new IllegalStateException(
                        "Host without a datacenter should not be in list of hosts for writing: "
                            + Json.toString(host));
                }
            }

        } else {

            this.hostsForReading.remove(host);

            if (this.multiRegionWritesEnabled) {
                assert this.hostsForReading == this.hostsForWriting;
            } else {
                this.hostsForWriting.remove(host);
            }
        }

        if (LOG.isWarnEnabled()) {

            if (this.multiRegionWritesEnabled) {
                if (this.hostsForReading.isEmpty()) {
                    LOG.warn("All nodes have now been removed: {}", toJson(this));
                }
            } else {
                if (this.hostsForReading.isEmpty()) {
                    LOG.warn("All nodes for reading have now been removed: {}", toJson(this));
                }
                if (this.hostsForWriting.isEmpty()) {
                    LOG.warn("All nodes for writing have now been removed: {}", toJson(this));
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("onDown -> {}", this);
        }
    }

    @Override
    public void onRemove(final Host host) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("onRemove({})", toJson(host));
        }
        this.onDown(host);
    }

    @Override
    public void onUp(final Host host) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp({})", toJson(host));
        }

        this.hostsForReading.add(host);

        if (this.multiRegionWritesEnabled) {
            assert this.hostsForReading == this.hostsForWriting;
        } else {
            if (this.contactPoints.contains(host)) {
                this.hostsForWriting.add(host);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("onUp -> {}", this);
        }
    }

    @Override
    public String toString() {
        return Json.toString(this);
    }

    // endregion

    // region Privates

    private static CosmosLoadBalancingPolicy buildFrom(final Builder builder) {
        return new CosmosLoadBalancingPolicy(builder.preferredRegions, builder.multiRegionWrites);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static List<Host> getContactPoints(@NonNull final Cluster cluster) {

        requireNonNull(cluster, "expected non-null cluster");
        final List<Host> contactPoints;

        try {
            contactPoints = (List<Host>) GET_CONTACT_POINTS.invoke(cluster.getMetadata());
        } catch (IllegalAccessException | InvocationTargetException error) {
            final String message = "Could not obtain contact points for cluster " + cluster + " due to: " + error;
            LOG.error("Could not obtain contact points from cluster {} due to: ", cluster, error);
            throw new IllegalStateException(message, error);
        }

        return contactPoints == null ? null : new ArrayList<>(contactPoints);
    }

    @NonNull
    private static List<Host> getContactPointsOrThrow(@NonNull final Cluster cluster) {

        final List<Host> contactPoints = getContactPoints(cluster);

        if (contactPoints == null) {
            final String message = "Could not obtain contact points for cluster " + cluster;
            LOG.error("{}: {}", message, toJson(cluster));
            throw new IllegalStateException(message);
        }

        return contactPoints;
    }

    private static boolean isReadRequest(final String query) {
        return query.toLowerCase(Locale.ROOT).startsWith("select");
    }

    private static boolean isReadRequest(final Statement statement) {
        if (statement instanceof RegularStatement) {
            if (statement instanceof SimpleStatement) {
                final SimpleStatement simpleStatement = (SimpleStatement) statement;
                return isReadRequest(simpleStatement.getQueryString());
            } else if (statement instanceof BuiltStatement) {
                final BuiltStatement builtStatement = (BuiltStatement) statement;
                return isReadRequest(builtStatement.getQueryString());
            }
        } else if (statement instanceof BoundStatement) {
            final BoundStatement boundStatement = (BoundStatement) statement;
            return isReadRequest(boundStatement.preparedStatement().getQueryString());
        } else if (statement instanceof BatchStatement) {
            return false;
        }

        return false;
    }

    // endregion

    // region Types

    /**
     * A builder for constructing {@link CosmosLoadBalancingPolicy} objects.
     */
    public static class Builder {

        private boolean multiRegionWrites = false;
        private List<String> preferredRegions = Collections.emptyList();

        /**
         * Constructs a new {@link CosmosLoadBalancingPolicy} object.
         *
         * @return a newly constructed {@link CosmosLoadBalancingPolicy} object.
         */
        public CosmosLoadBalancingPolicy build() {
            return CosmosLoadBalancingPolicy.buildFrom(this);
        }

        /**
         * Sets a value indicating whether multi-region writes are enabled.
         *
         * @param value {@code true} if multi-region writes are enabled.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withMultiRegionWrites(final boolean value) {
            this.multiRegionWrites = value;
            return this;
        }

        /**
         * Sets the preferred region list.
         * <p>
         * An immutable copy of the list is created when {@link #build} is called. This prevents modification to the
         * preferred regions list for a {@link CosmosLoadBalancingPolicy} instance.
         *
         * @param value A preferred region list.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withPreferredRegions(@NonNull final List<String> value) {
            this.preferredRegions = requireNonNull(value, "expected non-null value");
            return this;
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class PreferredRegionsComparator implements Comparator<Host> {

        private final Map<String, Integer> indexes;

        PreferredRegionsComparator(@NonNull final List<String> preferredRegions) {

            this.indexes = new HashMap<>(requireNonNull(preferredRegions, "expected non-null preferredRegions").size());
            int index = 0;

            for (final String region : preferredRegions) {
                this.indexes.put(region, index++);
            }
        }

        /**
         * Compares two {@linkplain Host hosts} based on an ordering that ensures nodes in preferred regions followed by
         * nodes that were specified as contact points sort before nodes that are neither.
         * <p>
         * Nodes that don't belong to datacenters in the preferred region list are compared alphabetically by datacenter
         * name, then by host ID. This results in predictable failover routing behavior. If all preferred regions are
         * down and no contact points are available, other datacenters will be considered in alphabetic order.
         *
         * @param x One {@linkplain Host host}.
         * @param y Another {@linkplain Host host}.
         *
         * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or
         * greater than the second.
         */
        @SuppressFBWarnings(value = "RC_REF_COMPARISON", justification = "Reference comparison is intentional")
        @SuppressWarnings("NumberEquality")
        @Override
        public int compare(@NonNull final Host x, @NonNull final Host y) {

            requireNonNull(x, "expected non-null x");
            requireNonNull(y, "expected non-null y");

            if (x == y) {
                return 0;
            }

            final String xDatacenter = x.getDatacenter();
            final String yDatacenter = y.getDatacenter();

            requireNonNull(xDatacenter, "expected non-null x::datacenter");
            requireNonNull(yDatacenter, "expected non-null y::datacenter");

            final int compareDatacenterNames = xDatacenter.compareTo(yDatacenter);

            if (compareDatacenterNames != 0) {

                final Integer xIndex = this.indexes.get(xDatacenter);
                final Integer yIndex = this.indexes.get(yDatacenter);

                if (xIndex != yIndex) {

                    if (xIndex == null) {
                        return -1;  // y is a preferred datacenter and x is not
                    }

                    if (yIndex == null) {
                        return 1;  // x is a preferred datacenter and y is not
                    }

                    final int result = Integer.compare(xIndex, yIndex);

                    if (result != 0) {
                        return result; // x and y are preferred datacenters and one has higher priority than the other
                    }
                }

                if (xIndex == null) {  // x and y are both not in a preferred datacenter
                    return compareDatacenterNames;
                }
            }

            // We distinguish x and y by Host ID because they're in the same datacenter

            final UUID xHostId = x.getHostId();
            final UUID yHostId = y.getHostId();

            requireNonNull(xHostId, "expected non-null x::hostId");
            requireNonNull(yHostId, "expected non-null y::hostId");

            return Objects.compare(x.getHostId(), y.getHostId(), UUID::compareTo);
        }
    }

    // endregion
}
