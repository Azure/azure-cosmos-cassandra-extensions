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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
 * If a {@code readDC} is specified, we prioritize nodes in the read datacenter for read requests. Either one of
 * {@code writeDC} or {@code globalEndpoint} must be specified to determine where write requests are routed.
 * If {@code writeDC} is specified, writes will be prioritized for that region. When {@code globalEndpoint} is
 * specified, write requests will be prioritized for the default write region. The {@code globalEndpoint} allows the
 * client to gracefully fail over by updating the default write region addresses. In this case
 * {@code dnsExpirationInSeconds} specifies the time to take to recover from the failover. By default, it is {@code 60}
 * seconds.
 */
public final class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {

    // region Fields

    private final int dnsExpirationInSeconds;
    private final String globalContactPoint;
    private final AtomicInteger index = new AtomicInteger();
    private final String readDC;
    private final String writeDC;
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private InetAddress[] localAddresses = null;
    private CopyOnWriteArrayList<Host> readLocalDCHosts;
    private CopyOnWriteArrayList<Host> remoteDcHosts;
    private CopyOnWriteArrayList<Host> writeLocalDcHosts;

    // endregion

    // region Constructors

    private CosmosLoadBalancingPolicy(
        final String readDC, final String writeDC, final String globalContactPoint, final int dnsExpirationInSeconds) {

        this.readDC = readDC;
        this.writeDC = writeDC;
        this.globalContactPoint = globalContactPoint;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;
    }

    // endregion

    // region Methods

    public static Builder builder() {
        return new Builder();
    }

    public void close() {
        // nothing to do
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * <p>This policy considers the nodes for the writeDC and the default write region at distance {@code LOCAL}.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(final Host host) {
        if (!this.writeDC.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDC)) {
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
        final CopyOnWriteArrayList<Host> readLocalDCAddresses = new CopyOnWriteArrayList<Host>();
        final CopyOnWriteArrayList<Host> writeLocalDCAddresses = new CopyOnWriteArrayList<Host>();
        final CopyOnWriteArrayList<Host> remoteDCAddresses = new CopyOnWriteArrayList<Host>();

        List<InetAddress> dnsLookupAddresses = new ArrayList<InetAddress>();
        if (!this.globalContactPoint.isEmpty()) {
            dnsLookupAddresses = Arrays.asList(this.getLocalAddresses());
        }

        for (final Host host : hosts) {
            if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)) {
                readLocalDCAddresses.add(host);
            }

            if ((!this.writeDC.isEmpty() && host.getDatacenter().equals(this.writeDC))
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
     *
     * <p>For read requests, the returned plan will always try each known host in the readDC first.
     * if none of the host is reachable, it will try all other hosts.
     * For writes and all other requests, the returned plan will always try each known host in the writeDC or the
     * default write region (looked up and cached from the globalEndpoint) first.
     * If none of the host is reachable, it will try all other hosts.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement      the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to try first for querying,
     * which one to use as failover, etc...
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

        if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)) {
            this.readLocalDCHosts.remove(host);
        }

        if (!this.writeDC.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDC)) {
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

        if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)) {
            this.readLocalDCHosts.addIfAbsent(host);
        }

        if (!this.writeDC.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDC)) {
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
     * DNS lookup based on the globalContactPoint and update localAddresses.
     *
     * @return
     */
    private InetAddress[] getLocalAddresses() {
        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.globalContactPoint);
                this.lastDnsLookupTime = System.currentTimeMillis() / 1000;
            } catch (final UnknownHostException ex) {
                // dns entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalArgumentException("The dns could not resolve the globalContactPoint the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private static CosmosLoadBalancingPolicy buildFrom(final Builder builder) {
        return new CosmosLoadBalancingPolicy(builder.readDC, builder.writeDC, builder.globalEndpoint, builder.dnsExpirationInSeconds);
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(final CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>)list.clone();
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
                final SimpleStatement simpleStatement = (SimpleStatement)statement;
                return isReadRequest(simpleStatement.getQueryString());
            } else if (statement instanceof BuiltStatement) {
                final BuiltStatement builtStatement = (BuiltStatement)statement;
                return isReadRequest(builtStatement.getQueryString());
            }
        } else if (statement instanceof BoundStatement) {
            final BoundStatement boundStatement = (BoundStatement)statement;
            return isReadRequest(boundStatement.preparedStatement().getQueryString());
        } else if (statement instanceof BatchStatement) {
            return false;
        }

        return false;
    }

    private void refreshHostsIfDnsExpired() {

        if (this.globalContactPoint.isEmpty() || (this.writeLocalDcHosts != null && !this.dnsExpired())) {
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
                throw new IllegalArgumentException("When the globalEndpoint is not specified, you need to provide both " +
                    "readDC and writeDC.");
            }
        } else {
            if (!builder.writeDC.isEmpty()) {
                throw new IllegalArgumentException("When the globalEndpoint is specified, you can't provide writeDC. Writes will go " +
                    "to the default write region when the globalEndpoint is specified.");
            }
        }
    }

    // endregion

    // region Types

    public static class Builder {

        private int dnsExpirationInSeconds = 60;
        private String globalEndpoint = "";
        private String readDC = "";
        private String writeDC = "";

        public CosmosLoadBalancingPolicy build() {
            validate(this);
            return CosmosLoadBalancingPolicy.buildFrom(this);
        }

        public Builder withDnsExpirationInSeconds(final int dnsExpirationInSeconds) {
            this.dnsExpirationInSeconds = dnsExpirationInSeconds;
            return this;
        }

        public Builder withGlobalEndpoint(final String globalEndpoint) {
            final int index = globalEndpoint.lastIndexOf(':');
            this.globalEndpoint = index == -1
                ? globalEndpoint
                : globalEndpoint.substring(0, index);
            return this;
        }

        public Builder withReadDC(final String readDC) {
            this.readDC = readDC;
            return this;
        }

        public Builder withWriteDC(final String writeDC) {
            this.writeDC = writeDC;
            return this;
        }
    }

    private static class HostIterator extends AbstractIterator<Host> {

        private final List<? extends Host> readHosts;
        private final List<? extends Host> remoteHosts;
        private final Statement statement;
        private final List<? extends Host> writeHosts;

        public int remainingRead;
        public int remainingWrite;
        private int idx;
        private int remainingRemote;

        public HostIterator(
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

        @SuppressWarnings("LoopStatementThatDoesntLoop")
        protected Host computeNext() {
            while (true) {
                if (this.remainingRead > 0 && isReadRequest(this.statement)) {
                    this.remainingRead--;
                    return this.readHosts.get(this.idx++ % this.readHosts.size());
                }

                if (this.remainingWrite > 0) {
                    this.remainingWrite--;
                    return this.writeHosts.get(this.idx++ % this.writeHosts.size());
                }

                if (this.remainingRemote > 0) {
                    this.remainingRemote--;
                    return this.remoteHosts.get(this.idx++ % this.remoteHosts.size());
                }

                return this.endOfData();
            }
        }
    }

    // endregion
}