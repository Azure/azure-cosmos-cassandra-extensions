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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.AbstractIterator;
import javafx.util.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CosmosDB failover aware Round-robin load balancing policy.
 * This allows the user to seamlessly failover the default write region.
 * This is very similar to DCAwareRoundRobinPolicy, with a difference that
 * it considers the nodes in the default write region at distance LOCAL.
 * All other nodes are considered at distance REMOTE.
 * The nodes in the default write region are retrieved from the global endpoint
 * of the account.
 */
public class CosmosFailoverAwareRRPolicy implements LoadBalancingPolicy
{
    private final AtomicInteger index = new AtomicInteger();
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private String globalContactPoint;
    private int dnsExpirationInSeconds;
    private Pair<CopyOnWriteArrayList<Host>, CopyOnWriteArrayList<Host>> hosts;
    private InetAddress[] localAddresses = null;

    public CosmosFailoverAwareRRPolicy(String globalContactPoint)
    {
        this(globalContactPoint, 60);
    }

    public CosmosFailoverAwareRRPolicy(String globalContactPoint, int dnsExpirationInSeconds)
    {
        this.globalContactPoint = globalContactPoint;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;
    }

    public void init(Cluster cluster, Collection<Host> hosts) {
        CopyOnWriteArrayList<Host> localDcAddresses = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> remoteDcAddresses = new CopyOnWriteArrayList<Host>();
        List<InetAddress> localAddressesFromLookup = Arrays.asList(getLocalAddresses());
        for (Host host : hosts) {
            if (localAddressesFromLookup.contains(host.getAddress()))
            {
                localDcAddresses.add(host);
            }
            else{
                remoteDcAddresses.add(host);
            }
        }

        this.hosts = new Pair(localDcAddresses, remoteDcAddresses);
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
    public HostDistance distance(Host host) {
        if (Arrays.asList(getLocalAddresses()).contains(host.getAddress()))
        {
            return HostDistance.LOCAL;
        }

        return HostDistance.REMOTE;
    }

    /**
     * Returns the hosts to use for a new query.
     *
     * <p>The returned plan will always try each known host in the default write region first, and then,
     * if none of the host is reachable, it will try the remote datacenter.
     * The order of the local node in the returned query plan will follow a
     * Round-robin algorithm.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to try first for querying,
     *     which one to use as failover, etc...
     */
    public Iterator<Host> newQueryPlan(String loggedKeyspace, final Statement statement)
    {
        Pair<CopyOnWriteArrayList<Host>, CopyOnWriteArrayList<Host>> allHosts = getHosts();
        final List<Host> localHosts = cloneList(allHosts.getKey());
        final List<Host> remoteHosts = cloneList(allHosts.getValue());
        final int startIdx = index.getAndIncrement();

        return new AbstractIterator<Host>() {
            private int idx = startIdx;
            private int remainingLocal = localHosts.size();
            private int remainingRemote = remoteHosts.size();

            protected Host computeNext() {
                while (true) {
                    if (remainingLocal > 0) {
                        remainingLocal--;
                        int c = idx++ % localHosts.size();
                        if (c < 0) {
                            c += localHosts.size();
                        }
                        return localHosts.get(c);
                    }

                    if (remainingRemote > 0) {
                        remainingRemote--;
                        int c = idx++ % remoteHosts.size();
                        if (c < 0) {
                            c += remoteHosts.size();
                        }
                        return remoteHosts.get(c);
                    }

                    return endOfData();
                }
            }
        };
    }

    public void onUp(Host host) {
        if (Arrays.asList(this.getLocalAddresses()).contains(host.getAddress()))
        {
            hosts.getKey().addIfAbsent(host);
            return;
        }

        hosts.getValue().addIfAbsent(host);
    }

    public void onDown(Host host) {
        if (Arrays.asList(this.getLocalAddresses()).contains(host.getAddress()))
        {
            hosts.getKey().remove(host);
            return;
        }

        hosts.getValue().remove(host);
    }

    public void onAdd(Host host) {
        onUp(host);
    }

    public void onRemove(Host host) {
        onDown(host);
    }

    public void close() {
        // nothing to do
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    private InetAddress[] getLocalAddresses()
    {
        if (this.localAddresses == null || dnsExpired()){
            try{
                this.localAddresses = InetAddress.getAllByName(globalContactPoint);
                this.lastDnsLookupTime = System.currentTimeMillis()/1000;
            }
            catch (UnknownHostException ex){

                // dns entry may be temporarily unavailable
                if (this.localAddresses == null){
                    throw new IllegalArgumentException("The dns could not resolve the globalContactPoint the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private Pair<CopyOnWriteArrayList<Host>, CopyOnWriteArrayList<Host>> getHosts() {
        if (hosts != null && !dnsExpired()){
            return hosts;
        }

        CopyOnWriteArrayList<Host> oldLocalDcHosts = this.hosts.getKey();
        CopyOnWriteArrayList<Host> newLocalDcHosts = this.hosts.getValue();

        List<InetAddress> localAddresses = Arrays.asList(getLocalAddresses());
        CopyOnWriteArrayList<Host> localDcHosts = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> remoteDcHosts = new CopyOnWriteArrayList<Host>();

        for (Host host: oldLocalDcHosts){
            if (localAddresses.contains(host.getAddress())){
                localDcHosts.addIfAbsent(host);
            }else{
                remoteDcHosts.addIfAbsent(host);
            }
        }

        for (Host host: newLocalDcHosts){
            if (localAddresses.contains(host.getAddress())){
                localDcHosts.addIfAbsent(host);
            }else{
                remoteDcHosts.addIfAbsent(host);
            }
        }

        return hosts = new Pair(localDcHosts, remoteDcHosts);
    }

    private boolean dnsExpired(){
        return System.currentTimeMillis()/1000 > lastDnsLookupTime + dnsExpirationInSeconds;
    }
}