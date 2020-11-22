// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PORT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.cleanUp;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.createSchema;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.display;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.uniqueName;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.write;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * This test illustrates use of the {@link CosmosRetryPolicy} class.
 * <h3>
 * Preconditions:
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
 * Side effects:
 * <ol>
 * <li>Creates a keyspace in the cluster with replication factor 3. To prevent collisions especially during CI test
 * runs, we generate a keyspace names of the form <b>downgrading_</b><i></i><random-uuid></i>. Should a keyspace by this
 * name already exists, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * </li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosRetryPolicyTest {

    // region Fields

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ONE;
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT = 300000;

    // endregion

    // region Methods

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        final Session session = connect(CosmosRetryPolicy.builder()
            .withMaxRetryCount(MAX_RETRY_COUNT)
            .withFixedBackOffTimeInMillis(FIXED_BACK_OFF_TIME)
            .withGrowingBackOffTimeInMillis(GROWING_BACK_OFF_TIME)
            .build());

        final String keyspaceName = uniqueName("downgrading");
        final String tableName = "sensor_data";

        try {

            assertThatCode(() ->
                createSchema(session, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            assertThatCode(() ->
                write(session, CONSISTENCY_LEVEL, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = TestCommon.read(session, CONSISTENCY_LEVEL, keyspaceName, tableName);
                display(rows);
            }).doesNotThrowAnyException();

        } finally {
            cleanUp(session, keyspaceName);
        }
    }

    @Test(groups = { "unit", "checkintest" }, timeOut = TIMEOUT)
    public void canRetryOnConnectionException() {

        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        final DriverException driverException = new ConnectionException(null, "retry");
        final Statement statement = new SimpleStatement("SELECT * FROM retry");

        for (int retryNumber = 0; retryNumber < MAX_RETRY_COUNT; retryNumber++) {
            final RetryDecision retryDecision = retryPolicy.onRequestError(statement,
                CONSISTENCY_LEVEL,
                driverException,
                retryNumber);
            assertThat(retryDecision.getType()).isEqualTo(RetryDecision.Type.RETRY);
        }
    }

    @Test(groups = { "unit", "checkintest" }, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithFixedBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(-1).build();
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    @Test(groups = { "unit", "checkintest" }, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    @Test(groups = { "unit", "checkintest" }, timeOut = TIMEOUT)
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        this.retry(retryPolicy, MAX_RETRY_COUNT + 1, MAX_RETRY_COUNT + 1, RetryDecision.Type.RETHROW);
    }

    // endregion

    // region Privates

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     */
    private static Session connect(final CosmosRetryPolicy retryPolicy) {

        final Cluster cluster = Cluster.builder()
            .withRetryPolicy(retryPolicy)
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

    /**
     * Tests a retry operation
     */
    private void retry(
        final CosmosRetryPolicy retryPolicy,
        final int retryNumberBegin,
        final int retryNumberEnd,
        final RetryDecision.Type expectedRetryDecisionType) {

        final DriverException driverException = new OverloadedException(null, "retry");
        final Statement statement = new SimpleStatement("SELECT * FROM retry");

        for (int retryNumber = retryNumberBegin; retryNumber < retryNumberEnd; retryNumber++) {

            final long expectedDuration = 1000000 * (retryPolicy.getMaxRetryCount() == -1
                ? FIXED_BACK_OFF_TIME
                : (long)retryNumber * GROWING_BACK_OFF_TIME);
            final long startTime = System.nanoTime();

            final RetryDecision retryDecision = retryPolicy.onRequestError(statement,
                CONSISTENCY_LEVEL,
                driverException,
                retryNumber);

            final long duration = System.nanoTime() - startTime;

            assertThat(retryDecision.getType()).isEqualTo(expectedRetryDecisionType);
            assertThat((double)duration).isGreaterThan(expectedDuration - 0.01 * expectedDuration);
        }
    }

    // endregion
}
