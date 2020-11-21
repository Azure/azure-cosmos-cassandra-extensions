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
import static com.microsoft.azure.cosmos.cassandra.TestCommon.close;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.createSchema;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.write;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * This test illustrates use of the {@link CosmosRetryPolicy} class.
 *
 * <p>Preconditions:
 *
 * <ul>
 * <li>An Apache Cassandra cluster is running and accessible through the contacts points
 * identified by #CONTACT_POINTS and #PORT.
 * </ul>
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
 * Notes:
 *
 * <ul>
 * <li>The downgrading logic here is similar to what {@code DowngradingConsistencyRetryPolicy}
 * does; feel free to adapt it to your application needs;
 * <li>You should never attempt to retry a non-idempotent write. See the driver's manual page on
 * idempotence for more information.
 * </ul>
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

    private Session session;

    // endregion

    // region Methods

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder()
            .withMaxRetryCount(MAX_RETRY_COUNT)
            .withFixedBackOffTimeInMillis(FIXED_BACK_OFF_TIME)
            .withGrowingBackOffTimeInMillis(GROWING_BACK_OFF_TIME)
            .build();

        assertThatCode(() -> this.connect(retryPolicy)).doesNotThrowAnyException();

        try {

            final String keyspaceName = "downgrading";
            final String tableName = "sensor_data";

            assertThatCode(() ->
                createSchema(this.session, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            assertThatCode(() ->
                write(this.session, CONSISTENCY_LEVEL, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = TestCommon.read(this.session, CONSISTENCY_LEVEL, keyspaceName, tableName);
                TestCommon.display(rows);
            }).doesNotThrowAnyException();

        } finally {
            close(this.session);
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
    private void connect(final CosmosRetryPolicy retryPolicy) {

        final Cluster cluster = Cluster.builder()
            .withRetryPolicy(retryPolicy)  // under test
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
