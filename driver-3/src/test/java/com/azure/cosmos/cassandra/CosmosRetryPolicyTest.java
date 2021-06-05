// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.provider.ValueSource;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * This test illustrates use of the {@link CosmosRetryPolicy} class.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li>A Cosmos DB Cassandra API account is required.
 * <li>These system variables or--alternatively--environment variables must be set.
 * <table><caption></caption>
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
 * runs, we generate a keyspace names of the form <b><code>downgrading_</code></b><i>&lt;random-uuid&gt;</i>. Should a
 * keyspace by this name already exists, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * </li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.
 * </ol>
 * <h3>
 * Notes</h3>
 * <ul>
 * <li>The downgrading logic here is similar to what {@code DowngradingConsistencyRetryPolicy} does; feel free to adapt
 * it to your application needs.
 * <li>You should never attempt to retry a non-idempotent write. See the driver's manual page on idempotence for more
 * information.
 * </ul>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosRetryPolicyTest {

    // region Fields

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ONE;
    private static final int FIXED_BACK_OFF_TIME = 5_000;
    private static final int GROWING_BACK_OFF_TIME = 1_000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT_IN_SECONDS = 30;

    // endregion

    // region Methods

    /**
     * Verifies that the {@link CosmosRetryPolicy} class integrates with DataStax Java Driver 3.
     */
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void canIntegrateWithCosmos() {

        final Session session = connect(CosmosRetryPolicy.builder()
            .withMaxRetryCount(MAX_RETRY_COUNT)
            .withFixedBackOffTimeInMillis(FIXED_BACK_OFF_TIME)
            .withGrowingBackOffTimeInMillis(GROWING_BACK_OFF_TIME)
            .build());

        final String keyspaceName = TestCommon.uniqueName("downgrading");
        final String tableName = "sensor_data";

        try {

            assertThatCode(() ->
                TestCommon.createSchema(session, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            assertThatCode(() ->
                TestCommon.write(session, CONSISTENCY_LEVEL, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = TestCommon.read(session, CONSISTENCY_LEVEL, keyspaceName, tableName);
                TestCommon.display(rows);
            }).doesNotThrowAnyException();

        } finally {
            TestCommon.cleanUp(session, keyspaceName);
        }
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries on a connection-related exception.
     */
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
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

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries with fixed backoff time.
     */
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void canRetryOverloadedExceptionWithFixedBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(-1).build();
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries with growing backoff time.
     */
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class rethrows when {@code max-retries} is exceeded.
     */
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        this.retry(retryPolicy, MAX_RETRY_COUNT + 1, MAX_RETRY_COUNT + 1, RetryDecision.Type.RETHROW);
    }

    // endregion

    // region Privates

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     */
    private static Session connect(final CosmosRetryPolicy retryPolicy) {

        final Cluster cluster = Cluster.builder()
            .withRetryPolicy(retryPolicy)
            .withCredentials(TestCommon.USERNAME, TestCommon.PASSWORD)
            .addContactPoint(TestCommon.GLOBAL_ENDPOINT_HOSTNAME)
            .withPort(TestCommon.GLOBAL_ENDPOINT_PORT)
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
                : (long) retryNumber * GROWING_BACK_OFF_TIME);
            final long startTime = System.nanoTime();

            final RetryDecision retryDecision = retryPolicy.onRequestError(statement,
                CONSISTENCY_LEVEL,
                driverException,
                retryNumber);

            final long duration = System.nanoTime() - startTime;

            assertThat(retryDecision.getType()).isEqualTo(expectedRetryDecisionType);
            assertThat((double) duration).isGreaterThan(expectedDuration - 0.01 * expectedDuration);
        }
    }

    // endregion
}
