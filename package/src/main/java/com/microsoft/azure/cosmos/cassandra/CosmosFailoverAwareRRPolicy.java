// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.AbstractIterator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @deprecated Please use {@link CosmosLoadBalancingPolicy} instead.
 * @since 0.12
 * <p>
 * Cosmos DB failover-aware round-robin load balancing policy.
 * <p>
 * This policy allows the user to seamlessly failover the default write region.
 * This is very similar to DCAwareRoundRobinPolicy, with a difference that
 * it considers the nodes in the default write region at distance LOCAL.
 * All other nodes are considered at distance REMOTE.
 * The nodes in the default write region are retrieved from the global endpoint
 * of the account, based on the dns refresh interval of 60 seconds by default.
 */
@Deprecated
public class CosmosFailoverAwareRRPolicy implements LoadBalancingPolicy {

    private final AtomicInteger index = new AtomicInteger();
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private final String globalContactPoint;
    private final int dnsExpirationInSeconds;
    private Map.Entry<CopyOnWriteArrayList<Host>, CopyOnWriteArrayList<Host>> hosts;
    private InetAddress[] localAddresses = null;

    /**
     * Creates a new CosmosDB failover aware round robin policy given the global endpoint.
     * Optionally specify dnsExpirationInSeconds, which defaults to 60 seconds.
     * @param globalContactPoint is the contact point of the account (e.g, *.cassandra.cosmos.azure.com)
     */
    public CosmosFailoverAwareRRPolicy(final String globalContactPoint) {
        this(globalContactPoint, 60);
    }

    /**
     * Creates a new CosmosDB failover aware round robin policy given the global endpoint.
     * @param globalContactPoint is the contact point of the account (e.g, *.cassandra.cosmos.azure.com)
     * @param dnsExpirationInSeconds specifies the dns refresh interval, which is 60 seconds by default.
     */
    public CosmosFailoverAwareRRPolicy(final String globalContactPoint, final int dnsExpirationInSeconds) {
        this.globalContactPoint = globalContactPoint;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;
    }

    @Override
    public void init(final Cluster cluster, final Collection<Host> hosts) {
        final CopyOnWriteArrayList<Host> localDcAddresses = new CopyOnWriteArrayList<Host>();
        final CopyOnWriteArrayList<Host> remoteDcAddresses = new CopyOnWriteArrayList<Host>();
        final List<InetAddress> localAddressesFromLookup = Arrays.asList(this.getLocalAddresses());
        for (final Host host : hosts) {
            if (localAddressesFromLookup.contains(host.getAddress())) {
                localDcAddresses.add(host);
            } else {
                remoteDcAddresses.add(host);
            }
        }

        this.hosts = new AbstractMap.SimpleEntry<>(localDcAddresses, remoteDcAddresses);
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * <p>This policy consider the nodes in the default write region as {@code LOCAL}.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(final Host host) {
        if (Arrays.asList(this.getLocalAddresses()).contains(host.getAddress())) {
            return HostDistance.LOCAL;
        }

        return HostDistance.REMOTE;
    }

    /**
     * Returns the hosts to use for a new query.
     *
     * <p>The returned plan will always try each known host in the default write region first, and then,
     * if none of the host is reachable, it will try all other regions.
     * The order of the local node in the returned query plan will follow a
     * Round-robin algorithm.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to try first for querying,
     *     which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

        final Map.Entry<CopyOnWriteArrayList<Host>, CopyOnWriteArrayList<Host>> allHosts = this.getHosts();
        final List<Host> localHosts = cloneList(allHosts.getKey());
        final List<Host> remoteHosts = cloneList(allHosts.getValue());
        final int startIdx = this.index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000) {
            this.index.set(0);
        }

        return new HostIterator(startIdx, localHosts, remoteHosts);
    }

    @Override
    public void onUp(final Host host) {
        if (Arrays.asList(this.getLocalAddresses()).contains(host.getAddress())) {
            this.hosts.getKey().addIfAbsent(host);
            return;
        }

        this.hosts.getValue().addIfAbsent(host);
    }

    @Override
    public void onDown(final Host host) {
        if (Arrays.asList(this.getLocalAddresses()).contains(host.getAddress())) {
            this.hosts.getKey().remove(host);
            return;
        }

        this.hosts.getValue().remove(host);
    }

    @Override
    public void onAdd(final Host host) {
        this.onUp(host);
    }

    @Override
    public void onRemove(final Host host) {
        this.onDown(host);
    }

    /**
     * Closes the current {@link CosmosFailoverAwareRRPolicy} object.
     */
    @Override
    public void close() {
        // nothing to do
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(final CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    /**
     * DNS lookup based on the {@link #globalContactPoint}.
     * <p>
     * If {@link #dnsExpirationInSeconds} has elapsed, the array of local addresses is also updated.
     *
     * @return value of {@link #localAddresses} which will have been updated, if {@link #dnsExpirationInSeconds}
     * has elapsed.
     */
    @SuppressWarnings("DuplicatedCode")
    private InetAddress[] getLocalAddresses() {

        if (this.localAddresses == null || this.dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(this.globalContactPoint);
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

    private Map.Entry<CopyOnWriteArrayList<Host>, CopyOnWriteArrayList<Host>> getHosts() {

        if (this.hosts != null && !this.dnsExpired()) {
            return this.hosts;
        }

        if (this.hosts == null) {
            throw new IllegalStateException("expected non-null "
                + CosmosFailoverAwareRRPolicy.class.getName()
                + ".hosts");
        }

        final CopyOnWriteArrayList<Host> oldLocalDcHosts = this.hosts.getKey();
        final CopyOnWriteArrayList<Host> newLocalDcHosts = this.hosts.getValue();

        final List<InetAddress> localAddresses = Arrays.asList(this.getLocalAddresses());
        final CopyOnWriteArrayList<Host> localDcHosts = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Host> remoteDcHosts = new CopyOnWriteArrayList<>();

        for (final Host host: oldLocalDcHosts) {
            if (localAddresses.contains(host.getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        for (final Host host: newLocalDcHosts) {
            if (localAddresses.contains(host.getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        this.hosts = new AbstractMap.SimpleEntry<>(localDcHosts, remoteDcHosts);
        return this.hosts;
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis() / 1000 > this.lastDnsLookupTime + this.dnsExpirationInSeconds;
    }

    private static class HostIterator extends AbstractIterator<Host> {

        private final List<? extends Host> localHosts;
        private final List<? extends Host> remoteHosts;
        private int idx;
        private int remainingLocal;
        private int remainingRemote;

        HostIterator(
            final int startIdx, final List<? extends Host> localHosts, final List<? extends Host> remoteHosts) {

            this.localHosts = localHosts;
            this.remoteHosts = remoteHosts;
            this.idx = startIdx;
            this.remainingLocal = localHosts.size();
            this.remainingRemote = remoteHosts.size();
        }

        @SuppressWarnings("LoopStatementThatDoesntLoop")
        protected Host computeNext() {

            while (true) {
                if (this.remainingLocal > 0) {
                    this.remainingLocal--;
                    return this.localHosts.get(this.idx++ % this.localHosts.size());
                }

                if (this.remainingRemote > 0) {
                    this.remainingRemote--;
                    return this.remoteHosts.get(this.idx++ % this.remoteHosts.size());
                }

                return this.endOfData();
            }
        }
    }
}
