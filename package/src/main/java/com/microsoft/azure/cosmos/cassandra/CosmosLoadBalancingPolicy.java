// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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

    private final int dnsExpirationInSeconds;
    private final String globalEndpoint;
    private final AtomicInteger index;
    private final String readDatacenter;
    private final String writeDatacenter;
    private long lastDnsLookupTime;

    private InetAddress[] localAddresses;
    private CopyOnWriteArrayList<Host> readLocalDCHosts;
    private CopyOnWriteArrayList<Host> remoteDcHosts;
    private CopyOnWriteArrayList<Host> writeLocalDcHosts;

    // endregion

    // region Constructors

    private CosmosLoadBalancingPolicy(
        final String readDatacenter,
        final String writeDatacenter,
        final String globalEndpoint,
        final int dnsExpirationInSeconds) {

        LOG.debug("globalEndpoint: '{}', readDatacenter: '{}', writeDatacenter: '{}', dnsExpirationInSeconds: '{}'",
            globalEndpoint,
            readDatacenter,
            writeDatacenter,
            dnsExpirationInSeconds);

        this.globalEndpoint = globalEndpoint;
        this.readDatacenter = readDatacenter;
        this.writeDatacenter = writeDatacenter;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;

        this.index = new AtomicInteger();
        this.lastDnsLookupTime = Long.MAX_VALUE;
    }

    // endregion

    // region Methods

    /**
     * Gets a newly created {@link CosmosLoadBalancingPolicy.Builder builder} object for constructing a {@link
     * CosmosLoadBalancingPolicy}.
     *
     * @return a newly created {@link CosmosLoadBalancingPolicy} builder instance.
     */
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
     * Return the {@link HostDistance} for the provided {@link Host}.
     * <p>
     * This policy considers the nodes for the write datacenter and the default write region at distance {@code LOCAL}.
     *
     * @param host the host of which to return the distance of.
     *
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(final Host host) {

        if (!this.writeDatacenter.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDatacenter)) {
                return HostDistance.LOCAL;
            }
        } else if (Arrays.asList(this.getLocalAddresses()).contains(host.getEndPoint().resolve().getAddress())) {
            return HostDistance.LOCAL;
        }

        return HostDistance.REMOTE;
    }

    /**
     * Initializes the list of hosts in read, write, local, and remote categories.
     */
    @Override
    public void init(final Cluster cluster, final Collection<Host> hosts) {

        final CopyOnWriteArrayList<Host> readLocalDCAddresses = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Host> writeLocalDCAddresses = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Host> remoteDCAddresses = new CopyOnWriteArrayList<>();

        final List<InetAddress> dnsLookupAddresses = this.globalEndpoint.isEmpty()
            ? Collections.emptyList()
            : Arrays.asList(this.getLocalAddresses());

        for (final Host host : hosts) {
            if (!this.readDatacenter.isEmpty() && host.getDatacenter().equals(this.readDatacenter)) {
                readLocalDCAddresses.add(host);
            }

            if ((!this.writeDatacenter.isEmpty() && host.getDatacenter().equals(this.writeDatacenter))
                || dnsLookupAddresses.contains(host.getEndPoint().resolve().getAddress())) {
                writeLocalDCAddresses.add(host);
            } else {
                remoteDCAddresses.add(host);
            }
        }

        this.readLocalDCHosts = readLocalDCAddresses;
        this.writeLocalDcHosts = writeLocalDCAddresses;
        this.remoteDcHosts = remoteDCAddresses;
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>For read requests, the returned plan will always try each known host in the readDC first.
     * if none of the host is reachable, it will try all other hosts. For writes and all other requests, the returned
     * plan will always try each known host in the writeDC or the default write region (looked up and cached from the
     * globalEndpoint) first. If none of the host is reachable, it will try all other hosts.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement      the query for which to build the plan.
     *
     * @return a new query plan, i.e. an iterator indicating which host to try first for querying, which one to use as
     * failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

        this.refreshHostsIfDnsExpired();

        final List<Host> readHosts = cloneList(this.readLocalDCHosts);
        final List<Host> writeHosts = cloneList(this.writeLocalDcHosts);
        final List<Host> remoteHosts = cloneList(this.remoteDcHosts);

        final int startIdx = this.index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000) {
            this.index.set(0);
        }

        return new HostIterator(readHosts, writeHosts, startIdx, remoteHosts, statement);
    }

    @Override
    public void onAdd(final Host host) {
        this.onUp(host);
    }

    @Override
    public void onDown(final Host host) {
        if (host == null || host.getDatacenter() == null) {
            return;
        }

        if (!this.readDatacenter.isEmpty() && host.getDatacenter().equals(this.readDatacenter)) {
            this.readLocalDCHosts.remove(host);
        }

        if (!this.writeDatacenter.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDatacenter)) {
                this.writeLocalDcHosts.remove(host);
            }
        } else if (Arrays.asList(this.getLocalAddresses()).contains(host.getEndPoint().resolve().getAddress())) {
            this.writeLocalDcHosts.remove(host);
        } else {
            this.remoteDcHosts.remove(host);
        }
    }

    @Override
    public void onRemove(final Host host) {
        this.onDown(host);
    }

    @Override
    public void onUp(final Host host) {

        if (host == null || host.getDatacenter() == null) {
            return;
        }

        if (!this.readDatacenter.isEmpty() && host.getDatacenter().equals(this.readDatacenter)) {
            this.readLocalDCHosts.addIfAbsent(host);
        }

        if (!this.writeDatacenter.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDatacenter)) {
                this.writeLocalDcHosts.addIfAbsent(host);
            }
        } else if (Arrays.asList(this.getLocalAddresses()).contains(host.getEndPoint().resolve().getAddress())) {
            this.writeLocalDcHosts.addIfAbsent(host);
        } else {
            this.remoteDcHosts.addIfAbsent(host);
        }
    }

    // endregion

    // region Privates

    /**
     * DNS lookup based on the {@link #globalEndpoint}.
     * <p>
     * If {@link #dnsExpirationInSeconds} has elapsed, the array of local addresses is also updated.
     *
     * @return value of {@link #localAddresses} which will have been updated, if {@link #dnsExpirationInSeconds} has
     * elapsed.
     */
    @SuppressWarnings("DuplicatedCode")
    private InetAddress[] getLocalAddresses() {
        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.globalEndpoint);
                this.lastDnsLookupTime = System.currentTimeMillis() / 1000;
            } catch (final UnknownHostException ex) {
                // dns entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalArgumentException(
                        "The DNS could not resolve the globalContactPoint the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private static CosmosLoadBalancingPolicy buildFrom(final Builder builder) {
        return new CosmosLoadBalancingPolicy(
            builder.readDC, builder.writeDC, builder.globalEndpoint, builder.dnsExpirationInSeconds);
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(final CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis() / 1000 > this.lastDnsLookupTime + this.dnsExpirationInSeconds;
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

    private void refreshHostsIfDnsExpired() {

        if (this.globalEndpoint.isEmpty() || (this.writeLocalDcHosts != null && !this.dnsExpired())) {
            return;
        }

        final CopyOnWriteArrayList<Host> oldLocalDcHosts = this.writeLocalDcHosts;
        final CopyOnWriteArrayList<Host> oldRemoteDcHosts = this.remoteDcHosts;

        final List<InetAddress> localAddresses = Arrays.asList(this.getLocalAddresses());
        final CopyOnWriteArrayList<Host> localDcHosts = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Host> remoteDcHosts = new CopyOnWriteArrayList<>();

        if (this.writeLocalDcHosts != null) {
            for (final Host host : oldLocalDcHosts) {
                if (localAddresses.contains(host.getEndPoint().resolve().getAddress())) {
                    localDcHosts.addIfAbsent(host);
                } else {
                    remoteDcHosts.addIfAbsent(host);
                }
            }
        }

        for (final Host host : oldRemoteDcHosts) {
            if (localAddresses.contains(host.getEndPoint().resolve().getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        this.writeLocalDcHosts = localDcHosts;
        this.remoteDcHosts = remoteDcHosts;
    }

    private static void validate(final Builder builder) {
        if (builder.globalEndpoint.isEmpty()) {
            if (builder.writeDC.isEmpty() || builder.readDC.isEmpty()) {
                throw new IllegalArgumentException("When the globalEndpoint is not specified, you need to provide both "
                    + "readDC and writeDC.");
            }
        } else {
            if (!builder.writeDC.isEmpty()) {
                throw new IllegalArgumentException("When the globalEndpoint is specified, you can't provide writeDC. "
                    + "Writes will go to the default write region when the globalEndpoint is specified.");
            }
        }
    }

    // endregion

    // region Types

    /**
     * A builder for constructing {@link CosmosLoadBalancingPolicy} objects.
     */
    public static class Builder {

        private int dnsExpirationInSeconds = 60;
        private String globalEndpoint = "";
        private String readDC = "";
        private String writeDC = "";

        /**
         * Constructs a new {@link CosmosLoadBalancingPolicy} object.
         *
         * @return a newly constructed {@link CosmosLoadBalancingPolicy} object.
         */
        public CosmosLoadBalancingPolicy build() {
            validate(this);
            return CosmosLoadBalancingPolicy.buildFrom(this);
        }

        /**
         * Sets the value of the DNS expiry interval.
         *
         * @param dnsExpirationInSeconds DNS expiry interval in seconds.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withDnsExpirationInSeconds(final int dnsExpirationInSeconds) {
            this.dnsExpirationInSeconds = dnsExpirationInSeconds;
            return this;
        }

        /**
         * Sets the global endpoint address.
         *
         * @param globalEndpoint a global endpoint address.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withGlobalEndpoint(final String globalEndpoint) {
            final int index = globalEndpoint.lastIndexOf(':');
            this.globalEndpoint = index == -1
                ? globalEndpoint
                : globalEndpoint.substring(0, index);
            return this;
        }

        /**
         * Sets the read datacenter name.
         *
         * @param readDC read datacenter name.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withReadDC(final String readDC) {
            this.readDC = readDC;
            return this;
        }

        /**
         * Sets the read datacenter name.
         *
         * @param writeDC write datacenter name.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withWriteDC(final String writeDC) {
            this.writeDC = writeDC;
            return this;
        }
    }

    private static class HostIterator extends AbstractIterator<Host> {

        // region Fields

        private final List<? extends Host> readHosts;
        private final List<? extends Host> remoteHosts;
        private final Statement statement;
        private final List<? extends Host> writeHosts;

        private int remainingRead;
        private int remainingWrite;
        private int idx;
        private int remainingRemote;

        // endregion

        // region Constructors

        HostIterator(
            final List<? extends Host> readHosts,
            final List<? extends Host> writeHosts,
            final int startIdx,
            final List<? extends Host> remoteHosts,
            final Statement statement) {

            this.readHosts = readHosts;
            this.writeHosts = writeHosts;
            this.remoteHosts = remoteHosts;
            this.statement = statement;
            this.remainingRead = readHosts.size();
            this.remainingWrite = writeHosts.size();
            this.idx = startIdx;
            this.remainingRemote = remoteHosts.size();
        }

        // endregion

        // region Methods

        @SuppressWarnings("LoopStatementThatDoesntLoop")
        protected Host computeNext() {

            while (true) {

                final boolean readRequest = isReadRequest(this.statement);
                final Host host;

                if (this.remainingRead > 0 && readRequest) {
                    this.remainingRead--;
                    host = this.readHosts.get(this.idx++ % this.readHosts.size());
                    LOG.debug("attempting read request in read datacenter at {}", host);
                    return host;
                }

                if (this.remainingWrite > 0) {
                    this.remainingWrite--;
                    host = this.writeHosts.get(this.idx++ % this.writeHosts.size());
                    LOG.debug("attempting {} request in write datacenter at {}", readRequest ? "read" : "write", host);
                    return host;
                }

                if (this.remainingRemote > 0) {
                    this.remainingRemote--;
                    host = this.remoteHosts.get(this.idx++ % this.remoteHosts.size());
                    LOG.debug("attempting {} request in remote datacenter at {}", readRequest ? "read" : "write", host);
                    return host;
                }

                return this.endOfData();
            }
        }

        // endregion
    }

    // endregion
}
