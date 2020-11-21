// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.testng.annotations.Test;

import static com.microsoft.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PORT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.close;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.display;
import static org.assertj.core.api.Assertions.assertThatCode;

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

    // region Fields

    private static final String KEYSPACE_NAME = "downgrading";
    private static final String TABLE_NAME = "sensor_data";
    private static final int TIMEOUT = 30000;

    private Session session;

    // endregion

    // region Methods

    @SuppressWarnings("deprecation")
    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        final LoadBalancingPolicy loadBalancingPolicy = new CosmosFailoverAwareRRPolicy(CONTACT_POINTS[0]);

        assertThatCode(() -> this.connect(loadBalancingPolicy)).doesNotThrowAnyException();

        try {
            assertThatCode(() ->
                TestCommon.createSchema(this.session, KEYSPACE_NAME, TABLE_NAME)
            ).doesNotThrowAnyException();

            assertThatCode(() ->
                TestCommon.write(this.session, KEYSPACE_NAME, TABLE_NAME)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = TestCommon.read(this.session, KEYSPACE_NAME, TABLE_NAME);
                display(rows);
            }).doesNotThrowAnyException();

        } finally {
            close(this.session);
        }
    }

    // endregion

    // region Privates

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     */
    private void connect(final LoadBalancingPolicy loadBalancingPolicy) {

        final Cluster cluster = Cluster.builder()
            .withLoadBalancingPolicy(loadBalancingPolicy)  // under test
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoints(CONTACT_POINTS)
            .withPort(PORT)
            .withSSL()
            .build();

        try {
            this.session = cluster.connect();
            System.out.println("Connected to cluster: " + cluster.getClusterName());
        } catch (final Throwable error) {
            cluster.close();
            throw error;
        }
    }

    // endregion
}