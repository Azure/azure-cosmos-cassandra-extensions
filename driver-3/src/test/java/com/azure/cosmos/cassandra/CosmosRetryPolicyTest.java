// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.implementation.Json;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.CoordinatorException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_HOSTNAME;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.cosmosClusterBuilder;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ONE;
    private static final int FIXED_BACK_OFF_TIME = 5_000;
    private static final int GROWING_BACK_OFF_TIME = 1_000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT_IN_SECONDS = 30;

    // endregion

    // region Methods

    /**
     * Verifies that the {@link CosmosRetryPolicy#onRequestError} method retries {@link OverloadedException} as
     * expected.
     */
    @SuppressFBWarnings({ "SIC_INNER_SHOULD_BE_STATIC_ANON" })
    @SuppressWarnings({ "UnstableApiUsage" })
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(value = 5, unit = MINUTES)
    public void canRetryAsExpectedWhenThrottled() {

        // TODO (DANOBLE) create and then drop <perf_ks>.<perf_tbl> here.

        final Cluster.Builder builder = cosmosClusterBuilder()
            .withClusterName("CosmosRetryPolicyTest.canRetryAsExpectedWhenThrottled")
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD)
            .withoutJMXReporting();

        try (Cluster cluster = builder.build()) {

            LOG.info("{}", Json.toString(cluster));

            try (Session session = cluster.connect()) {

                LOG.info("{}", Json.toString(session));

                final ResultSetFuture[] futures = (ResultSetFuture[]) Array.newInstance(ResultSetFuture.class, 100);

                final ConcurrentHashMap<EndPoint, ConcurrentHashMap<Class<? extends CoordinatorException>, Integer>>
                    endPoints = new ConcurrentHashMap<>();

                final ConcurrentHashMap<Class<? extends Throwable>, Integer>
                    errors = new ConcurrentHashMap<>();

                final AtomicInteger completions = new AtomicInteger();
                final AtomicInteger failures = new AtomicInteger();
                final String keyspaceName = "perf_ks";
                final String tableName = "perf_tbl";

                final Statement statement = QueryBuilder.select()
                    .all()
                    .from(keyspaceName, tableName)
                    .setReadTimeoutMillis(10 * TIMEOUT_IN_SECONDS * 1_000);

                final CountDownLatch countDownLatch = new CountDownLatch(futures.length);
                final Instant startTime = Instant.now();

                for (int i = 0; i < futures.length; i++) {

                    final ResultSetFuture future = session.executeAsync(statement);

                    Futures.addCallback(future, new FutureCallback<ResultSet>() {
                        @Override
                        public void onFailure(@NonNull final Throwable error) {

                            try {
                                failures.incrementAndGet();

                                if (error instanceof CoordinatorException) {

                                    final CoordinatorException coordinatorException = (CoordinatorException) error;
                                    final EndPoint coordinator = coordinatorException.getEndPoint();

                                    if (coordinator != null) {
                                        endPoints.compute(coordinator, (endPoint, coordinatorExceptions) -> {
                                            if (coordinatorExceptions == null) {
                                                coordinatorExceptions = new ConcurrentHashMap<>();
                                            }
                                            coordinatorExceptions.compute(
                                                coordinatorException.getClass(),
                                                (cls, count) -> count == null ? 1 : count + 1);
                                            return coordinatorExceptions;
                                        });
                                    }
                                }

                                errors.compute(error.getClass(), (cls, count) -> count == null ? 1 : count + 1);
                            } finally {
                                countDownLatch.countDown();
                            }
                        }

                        @Override
                        public void onSuccess(@Nullable final ResultSet resultSet) {
                            try {
                                assertThat(resultSet).isNotNull();
                                final Host host = resultSet.getExecutionInfo().getQueriedHost();
                                endPoints.computeIfAbsent(host.getEndPoint(), endPoint -> new ConcurrentHashMap<>());
                                completions.incrementAndGet();
                            } finally {
                                countDownLatch.countDown();
                            }
                        }
                    });

                    futures[i] = future;
                }

                try {
                    countDownLatch.await();
                } catch (final InterruptedException error) {
                    // ignored
                }

                final Duration elapsedTime = Duration.between(Instant.now(), startTime);
                final Metrics metrics = cluster.getMetrics();
                assertThat(metrics).isNotNull();

                final MetricRegistry metricRegistry = metrics.getRegistry();
                final Duration readTimeout = Duration.ofMillis(statement.getReadTimeoutMillis());

                LOG.info(
                    "{\"cql-requests\":{},\"completions\":{},\"failures\":{},\"session-errors\":{},"
                        + "\"endpoint-errors\":{},\"elapsed-time\":{},\"read-timeout\":{},"
                        + "\"metrics\":{\"gauges\":{},\"timers\":{},\"counters\":{}}}",
                    futures.length,
                    completions.get(),
                    failures.get(),
                    toJson(errors),
                    toJson(endPoints),
                    toJson(elapsedTime),
                    toJson(readTimeout),
                    toJson(metricRegistry.getGauges()),
                    toJson(metricRegistry.getTimers()),
                    toJson(metricRegistry.getCounters()));

                // Expected: all errors are the result of request timeouts or rethrown OverloadedException errors
                // We expect mostly, if not entirely rethrown OverloadedException errors

                assertThat(errors).allSatisfy((type, count) -> {
                    assertThat(type).isIn(TimeoutException.class, OverloadedException.class);
                });

                // Expected:
                // * All errors and retries are from the first preferred region in the list of preferred regions
                // * All retries are the result of other errors, a metric that tracks calls to #onServerError
                // We don't expect any regional outages while this test is running

                final String preferredRegion = PREFERRED_REGIONS.get(0);
                int preferredRegionErrorCount = 0;
                int preferredRegionRetryCount = 0;
                int preferredRegionRetriesOnOtherErrorCount = 0;

                for (final EndPoint endPoint : endPoints.keySet()) {

                    String datacenter = null;

                    for (final Host host : cluster.getMetadata().getAllHosts()) {
                        if (host.getEndPoint().equals(endPoint)) {
                            datacenter = host.getDatacenter();
                        }
                    }

                    if (Objects.equals(datacenter, preferredRegion)) {

                        final ConcurrentHashMap<Class<? extends CoordinatorException>, Integer>
                            endPointErrors = endPoints.get(endPoint);

                        assertThat(endPointErrors).isNotNull();

                        for (final int count : endPointErrors.values()) {
                            preferredRegionErrorCount += count;
                        }

                        final Counter retriesCounter = metrics.getErrorMetrics().getRetries();
                        assertThat(retriesCounter).isNotNull();
                        preferredRegionRetryCount += retriesCounter.getCount();

                        final Counter retriesOnOtherErrorCounter = metrics.getErrorMetrics().getRetriesOnOtherErrors();
                        assertThat(retriesOnOtherErrorCounter).isNotNull();
                        preferredRegionRetriesOnOtherErrorCount += retriesOnOtherErrorCounter.getCount();

                        break;
                    }
                }

                final Integer expectedErrorCount = errors.get(OverloadedException.class);

                assertThat(preferredRegionRetriesOnOtherErrorCount).isGreaterThan(0);
                assertThat(preferredRegionRetryCount).isEqualTo(preferredRegionRetriesOnOtherErrorCount);
                assertThat(preferredRegionErrorCount).isEqualTo(expectedErrorCount == null ? 0 : expectedErrorCount);
            }
        } catch (final AssertionError error) {
            // swallow it
        } catch (final Throwable error) {
            fail("unexpected error", error);
        }
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries on a connection-related exception.
     */
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
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
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    public void canRetryOverloadedExceptionWithFixedBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(-1).build();
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries with growing backoff time.
     */
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    @BeforeAll
    public static void init() {
        TestCommon.printTestParameters();
        CosmosLoadBalancingPolicy.builder(); // forces class initialization
    }

    @BeforeEach
    public void logTestName(final TestInfo info) {
        TestCommon.logTestName(info, LOG);
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class rethrows when {@code max-retries} is exceeded.
     */
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withMaxRetryCount(MAX_RETRY_COUNT).build();
        this.retry(retryPolicy, MAX_RETRY_COUNT + 1, MAX_RETRY_COUNT + 1, RetryDecision.Type.RETHROW);
    }

    // endregion

    // region Privates

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
