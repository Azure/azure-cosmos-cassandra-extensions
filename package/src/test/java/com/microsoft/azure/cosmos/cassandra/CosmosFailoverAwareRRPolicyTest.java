// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.testng.annotations.Test;

import static com.microsoft.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PORT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.cleanUp;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.createSchema;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.display;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.read;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.uniqueName;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.write;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * This test illustrates use of the {@link CosmosFailoverAwareRRPolicy} class.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li>A Cosmos DB Cassandra API account is required.
 * <li>These system variables or--alternatively--environment variables must be set.
 * <table>
 * <thead>
 * <tr>
 * <th>System variable</th>
 * <th>Environment variable</th>
 * <th>Description</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 * <td>azure.cosmos.cassandra.global-endpoint</td>
 * <td>AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT</td>
 * <td>Global endpoint address (e.g., "database-account.cassandra.cosmos.azure.com:10350")</td>
 * </tr>
 * <tr>
 * <td>azure.cosmos.cassandra.username</td>
 * <td>AZURE_COSMOS_CASSANDRA_USERNAME</td>
 * <td>Username for authentication</td>
 * </tr>
 * <tr>
 * <td>azure.cosmos.cassandra.password</td>
 * <td>AZURE_COSMOS_CASSANDRA_PASSWORD</td>
 * <td>Password for authentication</td>
 * </tr>
 * </tbody>
 * </table>
 * </ol>
 * <h3>
 * Side effects</h3>
 * <ol>
 * <li>Creates a keyspace in the cluster with replication factor 3. To prevent collisions especially during CI test
 * runs, we generate a keyspace names of the form <b>downgrading_</b><i>&lt;random-uuid&gt;</i>. Should a keyspace by
 * this name already exists, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * </li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosFailoverAwareRRPolicyTest {

    // region Fields

    private static final String KEYSPACE_NAME = uniqueName("downgrading");
    private static final String TABLE_NAME = "sensor_data";
    private static final int TIMEOUT = 30000;

    // endregion

    // region Methods

    /**
     *
     */
    @SuppressWarnings("deprecation")
    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        final Session session = connect(new CosmosFailoverAwareRRPolicy(CONTACT_POINTS[0]));

        try {
            assertThatCode(() ->
                createSchema(session, KEYSPACE_NAME, TABLE_NAME)
            ).doesNotThrowAnyException();

            assertThatCode(() ->
                write(session, ConsistencyLevel.ONE, KEYSPACE_NAME, TABLE_NAME)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = read(session, ConsistencyLevel.ONE, KEYSPACE_NAME, TABLE_NAME);
                display(rows);
            }).doesNotThrowAnyException();

        } finally {
            cleanUp(session, KEYSPACE_NAME);
        }
    }

    // endregion

    // region Privates

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     * @param loadBalancingPolicy the load balancing policy under test.
     */
    private static Session connect(final LoadBalancingPolicy loadBalancingPolicy) {

        final Cluster cluster = Cluster.builder()
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoints(CONTACT_POINTS)
            .withPort(PORT)
            .withSSL()
            .build();

        try {
            return cluster.connect();
        } catch (final Throwable error) {
            cluster.close();
            throw error;
        }
    }

    // endregion
}
