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

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.collect.AbstractIterator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a Cassandra {@link LoadBalancingPolicy} with an option to specify readDC and writeDC
 * to route read and write requests to their corresponding data centers.
 * If readDC is specified, we prioritize nodes in the readDC for read requests.
 * writeDC or globalEndpoint needs to be specified for computing which nodes to send write requests.
 * When globalEndpoint is specified, the write requests will be prioritized for the default write region.
 * globalEndpoint allows the client to gracefully failover when the default write region is changed.
 * dnsExpirationInSeconds is essentially the max duration to recover from the failover. By default, it is 60 seconds.
 */
public final class CosmosDBLoadBalancingPolicy implements LoadBalancingPolicy {

    /**
     * Initializes the list of hosts in read, write, local, and remote categories.
     */
    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        CopyOnWriteArrayList<Host> readLocalDCAddresses = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> writeLocalDCAddresses = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> remoteDCAddresses = new CopyOnWriteArrayList<Host>();

        List<InetAddress> dnsLookupAddresses = new ArrayList<InetAddress>();
        if (!globalContactPoint.isEmpty()) {
            dnsLookupAddresses = Arrays.asList(getLocalAddresses());
        }

        for (Host host : hosts) {
            if (!this.readDC.isEmpty() && host.getDatacenter().equals(readDC)) {
                readLocalDCAddresses.add(host);
            }

            if ((!this.writeDC.isEmpty() && host.getDatacenter().equals(writeDC))
                    || dnsLookupAddresses.contains(host.getAddress())) {
                writeLocalDCAddresses.add(host);
            }
            else {
                remoteDCAddresses.add(host);
            }
        }

        this.readLocalDCHosts = readLocalDCAddresses;
        this.writeLocalDCHosts = writeLocalDCAddresses;
        this.remoteDCHosts = remoteDCAddresses;
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * <p>This policy consider the nodes for the writeDC and the default write region at distance {@code LOCAL}.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        if (!this.writeDC.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDC)) {
                return HostDistance.LOCAL;
            }
        } else if (Arrays.asList(getLocalAddresses()).contains(host.getAddress())) {
            return HostDistance.LOCAL;
        }

        return HostDistance.REMOTE;
    }

    /**
     * Returns the hosts to use for a new query.
     *
     * <p>For read requests, the returned plan will always try each known host in the readDC first.
     * if none of the host is reachable, it will try all other hosts.
     * For writes and all other requests, the returned plan will always try each known host in the writeDC or the
     * default write region (looked up and cached from the globalEndpoint) first.
     * If none of the host is reachable, it will try all other hosts.
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to try first for querying,
     *     which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, final Statement statement) {
        refreshHostsIfDnsExpired();

        final List<Host> readHosts = cloneList(this.readLocalDCHosts);
        final List<Host> writeHosts = cloneList(this.writeLocalDCHosts);
        final List<Host> remoteHosts = cloneList(this.remoteDCHosts);

        final int startIdx = index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000) {
            index.set(0);
        }

        return new AbstractIterator<Host>() {
            private int idx = startIdx;
            public int remainingRead = readHosts.size();
            public int remainingWrite = writeHosts.size();
            private int remainingRemote = remoteHosts.size();

            protected Host computeNext() {
                while (true) {
                    if (remainingRead > 0 && isReadRequest(statement)) {
                        remainingRead--;
                        return readHosts.get(idx ++ % readHosts.size());
                    }

                    if (remainingWrite > 0) {
                        remainingWrite--;
                        return writeHosts.get(idx ++ % writeHosts.size());
                    }

                    if (remainingRemote > 0) {
                        remainingRemote--;
                        return remoteHosts.get(idx ++ % remoteHosts.size());
                    }

                    return endOfData();
                }
            }
        };
    }

    @Override
    public void onUp(Host host) {
        if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)) {
            this.readLocalDCHosts.addIfAbsent(host);
        }

        if (!this.writeDC.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDC)) {
                this.writeLocalDCHosts.addIfAbsent(host);
            }
        } else if (Arrays.asList(getLocalAddresses()).contains(host.getAddress())) {
                this.writeLocalDCHosts.addIfAbsent(host);
        } else {
            this.remoteDCHosts.addIfAbsent(host);
        }
    }

    @Override
    public void onDown(Host host) {
        if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)) {
            this.readLocalDCHosts.remove(host);
        }

        if (!this.writeDC.isEmpty()) {
            if (host.getDatacenter().equals(this.writeDC)) {
                this.writeLocalDCHosts.remove(host);
            }
        } else if (Arrays.asList(getLocalAddresses()).contains(host.getAddress())) {
            this.writeLocalDCHosts.remove(host);
        } else {
            this.remoteDCHosts.remove(host);
        }
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }

    public void close() {
        // nothing to do
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String readDC = "";
        private String writeDC = "";
        private String globalEndpoint = "";
        private int dnsExpirationInSeconds = 60;

        public Builder withReadDC(String readDC) {
            this.readDC = readDC;
            return this;
        }

        public Builder withWriteDC(String writeDC) {
            this.writeDC = writeDC;
            return this;
        }

        public Builder withGlobalEndpoint(String globalEndpoint) {
            this.globalEndpoint = globalEndpoint;
            return this;
        }

        public Builder withDnsExpirationInSeconds(int dnsExpirationInSeconds) {
            this.dnsExpirationInSeconds = dnsExpirationInSeconds;
            return this;
        }

        public CosmosDBLoadBalancingPolicy build() {
            validate(this);
            return CosmosDBLoadBalancingPolicy.buildFrom(this);
        }
    }

    private CosmosDBLoadBalancingPolicy(String readDC, String writeDC, String globalContactPoint, int dnsExpirationInSeconds) {
        this.readDC = readDC;
        this.writeDC = writeDC;
        this.globalContactPoint = globalContactPoint;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;
    }

    private final AtomicInteger index = new AtomicInteger();
    private long lastDnsLookupTime = Long.MIN_VALUE;

    private InetAddress[] localAddresses = null;
    private CopyOnWriteArrayList<Host> readLocalDCHosts;
    private CopyOnWriteArrayList<Host> writeLocalDCHosts;
    private CopyOnWriteArrayList<Host> remoteDCHosts;

    private String readDC;
    private String writeDC;
    private String globalContactPoint;
    private int dnsExpirationInSeconds;


    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    /**
     * DNS lookup based on the globalContactPoint and update localAddresses.
     * @return
     */
    private InetAddress[] getLocalAddresses() {
        if (this.localAddresses == null || dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(globalContactPoint);
                this.lastDnsLookupTime = System.currentTimeMillis()/1000;
            }
            catch (UnknownHostException ex) {
                // dns entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalArgumentException("The dns could not resolve the globalContactPoint the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private void refreshHostsIfDnsExpired() {
        if (this.globalContactPoint.isEmpty() || (this.writeLocalDCHosts != null && !dnsExpired())) {
            return;
        }

        CopyOnWriteArrayList<Host> oldLocalDCHosts = this.writeLocalDCHosts;
        CopyOnWriteArrayList<Host> oldRemoteDCHosts = this.remoteDCHosts;

        List<InetAddress> localAddresses = Arrays.asList(getLocalAddresses());
        CopyOnWriteArrayList<Host> localDcHosts = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> remoteDcHosts = new CopyOnWriteArrayList<Host>();

        for (Host host: oldLocalDCHosts) {
            if (localAddresses.contains(host.getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        for (Host host: oldRemoteDCHosts) {
            if (localAddresses.contains(host.getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        this.writeLocalDCHosts = localDcHosts;
        this.remoteDCHosts = remoteDcHosts;
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis()/1000 > lastDnsLookupTime + dnsExpirationInSeconds;
    }

    private static boolean isReadRequest(Statement statement) {
        if (statement instanceof RegularStatement) {
            if (statement instanceof SimpleStatement) {
                SimpleStatement simpleStatement = (SimpleStatement) statement;
                return isReadRequest(simpleStatement.getQueryString());
            } else if (statement instanceof BuiltStatement) {
                BuiltStatement builtStatement = (BuiltStatement) statement;
                return isReadRequest(builtStatement.getQueryString());
            }
        } else if (statement instanceof BoundStatement) {
            BoundStatement boundStatement = (BoundStatement) statement;
            return isReadRequest(boundStatement.preparedStatement().getQueryString());
        } else if (statement instanceof BatchStatement) {
            return false;
        }

        return false;
    }

    private static boolean isReadRequest(String query) {
        return query.toLowerCase().startsWith("select");
    }

    private static void validate(Builder builder) {
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

    private static CosmosDBLoadBalancingPolicy buildFrom(Builder builder) {
        return new CosmosDBLoadBalancingPolicy(builder.readDC, builder.writeDC, builder.globalEndpoint, builder.dnsExpirationInSeconds);
    }
}