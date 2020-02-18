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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.fail;

/**
 * This test illustrates use of the {@link CosmosFailoverAwareRRPolicy} class.
 *
 * <p>Preconditions:
 *
 * <ul>
 * <li> CosmosDB Cassandra account must be created.
 * <li> TestCommon.CONTACT_POINTS is the the global endpoint (e.g *.cassandra.cosmos.azure.com).
 * <li> TestCommon.PORT is 10350.
 * <p>
 * Side effects:
 *
 * <ol>
 * <li>Creates a new keyspace {@code downgrading} in the cluster, with replication factor 3. If a
 * keyspace with this name already exists, it will be reused;
 * <li>Creates a new table {@code downgrading.sensor_data}. If a table with that name exists
 * already, it will be reused;
 * <li>Inserts a few rows, downgrading the consistency level if the operation fails;
 * <li>Queries the table, downgrading the consistency level if the operation fails;
 * <li>Displays the results on the console.
 * </ol>
 * <p>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosFailoverAwareRRPolicyTest {

    @Test(groups = {"integration", "checkintest"}, timeOut = TIMEOUT)
    public void cosmosLBPBasicTest() {
        LoadBalancingPolicy loadBalancingPolicy = new CosmosFailoverAwareRRPolicy(TestCommon.CONTACT_POINTS[0]);

        try {
            this.connect(TestCommon.CONTACT_POINTS, TestCommon.PORT, loadBalancingPolicy);
        } catch (Exception error) {
            fail(String.format("connect failed with %s: %s", error.getClass().getCanonicalName(), error));
        }

        try {
            try {
                TestCommon.createSchema(session, keyspaceName, tableName);
            } catch (Exception error) {
                fail(String.format("createSchema failed: %s", error));
            }
            try {
                TestCommon.write(session, keyspaceName, tableName);

            } catch (Exception error) {
                fail(String.format("write failed: %s", error));
            }
            try {
                ResultSet rows = TestCommon.read(session, keyspaceName, tableName);
                TestCommon.display(rows);

            } catch (Exception error) {
                fail(String.format("read failed: %s", error));
            }

        } finally {
            this.close();
        }
    }

    private static final int TIMEOUT = 30000;

    private Cluster cluster;
    private Session session;
    private String keyspaceName = "downgrading";
    private String tableName = "sensor_data";

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     * @param contactPoints the contact points to use.
     * @param port          the port to use.
     */
    private void connect(String[] contactPoints, int port, LoadBalancingPolicy loadBalancingPolicy) {
        cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).withLoadBalancingPolicy(loadBalancingPolicy).build();
        System.out.println("Connected to cluster: " + cluster.getClusterName());
        session = cluster.connect();
    }

    /**
     * Closes the session and the cluster.
     */
    private void close() {
        if (session != null) {
            session.close();
            cluster.close();
        }
    }
}