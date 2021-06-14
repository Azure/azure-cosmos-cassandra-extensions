// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.implementation.Json;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import org.junit.jupiter.api.AfterAll;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.KEYSPACE_NAME;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.REGIONAL_ENDPOINTS;
import static com.azure.cosmos.cassandra.TestCommon.createSchema;
import static com.azure.cosmos.cassandra.TestCommon.display;
import static com.azure.cosmos.cassandra.TestCommon.read;
import static com.azure.cosmos.cassandra.TestCommon.uniqueName;
import static com.azure.cosmos.cassandra.TestCommon.write;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric.RETRIES;
import static com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric.RETRIES_ON_OTHER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
public final class CosmosRetryPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosRetryPolicyTest.class);

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    private static final int FIXED_BACK_OFF_TIME = CosmosRetryPolicyOption.FIXED_BACKOFF_TIME
        .getDefaultValue(Integer.class);

    private static final int GROWING_BACK_OFF_TIME = CosmosRetryPolicyOption.GROWING_BACKOFF_TIME
        .getDefaultValue(Integer.class);

    private static final int MAX_RETRIES = CosmosRetryPolicyOption.MAX_RETRIES.getDefaultValue(Integer.class);

    private static final String TABLE_NAME = uniqueName("sensor_data_");
    private static final int TIMEOUT_IN_SECONDS = 60;

    private static CqlSession session = null;

    // endregion

    // region Methods

    /**
     * Verifies that the {@link CosmosRetryPolicy} class integrates with DataStax Java Driver 4.
     */
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    public void canIntegrateWithCosmos() {

        assertThatCode(() -> createSchema(session, KEYSPACE_NAME, TABLE_NAME, 400)).doesNotThrowAnyException();
        assertThatCode(() -> write(session, KEYSPACE_NAME, TABLE_NAME, CONSISTENCY_LEVEL)).doesNotThrowAnyException();

        assertThatCode(() -> {
            final ResultSet rows = read(session, KEYSPACE_NAME, TABLE_NAME, CONSISTENCY_LEVEL);
            display(rows);
        }).doesNotThrowAnyException();
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries with fixed backoff time.
     */
    @Test
    @Tag("checkin")
    @Tag("unit")
    public void canRetryOverloadedExceptionWithFixedBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(-1);
        this.retry(retryPolicy, 0, MAX_RETRIES, RetryDecision.RETRY_SAME);
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class faithfully executes retries with growing backoff time.
     */
    @Test
    @Tag("checkin")
    @Tag("unit")
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRIES);
        this.retry(retryPolicy, 0, MAX_RETRIES, RetryDecision.RETRY_SAME);
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy#onErrorResponse} method retries {@link OverloadedException} as
     * expected.
     */
    @SuppressWarnings("unchecked")
    @Test
    @Tag("checkin")
    @Tag("integration")
    @Timeout(10 * TIMEOUT_IN_SECONDS)
    public void canRetryAsExpectedWhenThrottled() {

        // TODO (DANOBLE) create and then drop <perf_ks>.<perf_tbl> here.

        LOG.info("{}", Json.toString(session));

        final CompletableFuture<AsyncResultSet>[] futures = (CompletableFuture<AsyncResultSet>[]) Array.newInstance(
            CompletableFuture.class,
            500);

        final ConcurrentHashMap<Node, ConcurrentHashMap<Class<? extends DriverException>, Integer>>
            nodes = new ConcurrentHashMap<>();

        final ConcurrentHashMap<Class<? extends Throwable>, Integer>
            errors = new ConcurrentHashMap<>();

        final AtomicInteger completions = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final String keyspaceName = "perf_ks";
        final String tableName = "perf_tbl";

        final Statement<SimpleStatement> statement = QueryBuilder.selectFrom(keyspaceName, tableName)
            .all()
            .build()
            .setTimeout(Duration.ofSeconds(10 * TIMEOUT_IN_SECONDS));

        final Instant startTime = Instant.now();

        for (int i = 0; i < futures.length; i++) {

            futures[i] = session.executeAsync(statement).toCompletableFuture().whenComplete(
                (asyncResultSet, error) -> {
                    if (error == null) {
                        final Node coordinator = asyncResultSet.getExecutionInfo().getCoordinator();
                        nodes.computeIfAbsent(coordinator, node -> new ConcurrentHashMap<>());
                        completions.incrementAndGet();
                        return;
                    }
                    failures.incrementAndGet();
                    if (error instanceof DriverException) {
                        final DriverException driverException = (DriverException) error;
                        final Node coordinator = driverException.getExecutionInfo().getCoordinator();
                        if (coordinator != null) {
                            nodes.compute(coordinator, (node, driverExceptions) -> {
                                if (driverExceptions == null) {
                                    driverExceptions = new ConcurrentHashMap<>();
                                }
                                driverExceptions.compute(driverException.getClass(), (cls, count) ->
                                    count == null ? 1 : count + 1);
                                return driverExceptions;
                            });
                        }
                    }
                    errors.compute(error.getClass(), (cls, count) -> count == null ? 1 : count + 1);
                });
        }

        try {
            CompletableFuture.allOf(futures).join();
        } catch (final CompletionException error) {
            // ignored
        }

        final Duration elapsedTime = Duration.between(Instant.now(), startTime);
        final Metrics metrics = session.getMetrics().orElse(null);
        assertThat(metrics).isNotNull();

        final MetricRegistry metricRegistry = metrics.getRegistry();

        final Duration requestTimeout = statement.getTimeout();

        final int maxRequestsPerSecond = session.getContext()
            .getConfig()
            .getDefaultProfile()
            .getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND);

        LOG.info(
            "{\"cql-requests\":{},\"completions\":{},\"failures\":{},\"session-errors\":{},\"node-errors\":{},"
                + "\"elapsed-time\":{},\"request-timeout\":{},\"max-requests-per-second\":{},\"metrics\":{"
                + "\"gauges\":{},\"timers\":{},\"counters\":{}}}",
            futures.length,
            completions.get(),
            failures.get(),
            toJson(errors),
            toJson(nodes),
            toJson(elapsedTime),
            toJson(requestTimeout),
            toJson(maxRequestsPerSecond),
            toJson(metricRegistry.getGauges()),
            toJson(metricRegistry.getTimers()),
            toJson(metricRegistry.getCounters()));

        // Expected: all errors are the result of request timeouts or rethrown OverloadedException errors
        // We expect mostly, if not entirely rethrown OverloadedException errors

        assertThat(errors).allSatisfy((type, count) -> {
            assertThat(type).isIn(DriverTimeoutException.class, OverloadedException.class);
        });

        // Expected: all errors are from the first preferred region in the list of preferred regions
        // We don't expect any regional outages while this test is running

        final String preferredRegion = PREFERRED_REGIONS.get(0);
        int preferredRegionErrorCount = 0;
        int preferredRegionRetryCount = 0;
        int preferredRegionRetriesOnOtherErrorCount = 0;

        for (final Node node : nodes.keySet()) {
            if (Objects.equals(node.getDatacenter(), preferredRegion)) {
                final ConcurrentHashMap<Class<? extends DriverException>, Integer> nodeErrors = nodes.get(node);
                assertThat(nodeErrors).isNotNull();
                for (final int count : nodeErrors.values()) {
                    preferredRegionErrorCount += count;
                }
                final Optional<Counter> retriesCounter = metrics.getNodeMetric(node, RETRIES);
                assertThat(retriesCounter).isPresent();
                preferredRegionRetryCount += retriesCounter.get().getCount();
                final Optional<Counter> retriesOnOtherErrorCounter = metrics.getNodeMetric(node, RETRIES_ON_OTHER_ERROR);
                assertThat(retriesOnOtherErrorCounter).isPresent();
                preferredRegionRetriesOnOtherErrorCount += retriesOnOtherErrorCounter.get().getCount();
                break;
            }
        }

        final Integer expectedErrorCount = errors.get(OverloadedException.class);

        assertThat(preferredRegionRetriesOnOtherErrorCount).isGreaterThan(0);
        assertThat(preferredRegionRetryCount).isEqualTo(preferredRegionRetriesOnOtherErrorCount);
        assertThat(preferredRegionErrorCount).isEqualTo(expectedErrorCount == null ? 0 : expectedErrorCount);
    }

    /**
     * Closes the {@link #session} for testing {@link CosmosRetryPolicy} after dropping {@link TestCommon#KEYSPACE_NAME}, if it
     * exists.
     */
    @AfterAll
    @Timeout(TIMEOUT_IN_SECONDS)
    public static void cleanUp() {
        if (session != null && !session.isClosed()) {
            try {
                session.execute(SchemaBuilder.dropTable(KEYSPACE_NAME, TABLE_NAME).ifExists().build());
            } finally {
                session.close();
            }
        }
    }

    /**
     * Opens the {@link #session} for testing {@link CosmosRetryPolicy}.
     *
     * This method also verifies that the resulting session is configured and connected as expected.
     */
    @BeforeAll
    @Timeout(TIMEOUT_IN_SECONDS)
    public static void connect() {
        session = checkState(CqlSession.builder().build());
    }

    /**
     * Logs the name of each test before it is executed.
     *
     * @param info Test info.
     */
    @BeforeEach
    public void logTestName(final TestInfo info) {
        LOG.info("---------------------------------------------------------------------------------------------------");
        LOG.info("{}", info.getTestMethod().orElseGet(() -> fail("expected test to be called with test method")));
        LOG.info("---------------------------------------------------------------------------------------------------");
    }

    /**
     * Verifies that the {@link CosmosRetryPolicy} class rethrows when {@code max-retries} is exceeded.
     */
    @Test
    @Tag("checkin")
    @Tag("unit")
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRIES);
        this.retry(retryPolicy, MAX_RETRIES + 1, MAX_RETRIES + 1, RetryDecision.RETHROW);
    }

    // endregion

    // region Privates

    private static CqlSession checkState(final CqlSession session) {

        final DriverContext context = session.getContext();
        final DriverExecutionProfile profile = context.getConfig().getDefaultProfile();

        final RetryPolicy retryPolicy = context.getRetryPolicy(profile.getName());

        assertThat(retryPolicy.getClass()).isEqualTo(CosmosRetryPolicy.class);

        assertThat(((CosmosRetryPolicy) retryPolicy).getFixedBackOffTimeInMillis())
            .isEqualTo(profile.getInt(CosmosRetryPolicyOption.FIXED_BACKOFF_TIME));

        assertThat(((CosmosRetryPolicy) retryPolicy).getGrowingBackOffTimeInMillis())
            .isEqualTo(profile.getInt(CosmosRetryPolicyOption.GROWING_BACKOFF_TIME));

        assertThat(((CosmosRetryPolicy) retryPolicy).getMaxRetryCount())
            .isEqualTo(profile.getInt(CosmosRetryPolicyOption.MAX_RETRIES));

        assertThat(((CosmosRetryPolicy) retryPolicy).isRetryReadTimeoutsEnabled())
            .isEqualTo(profile.getBoolean(CosmosRetryPolicyOption.RETRY_READ_TIMEOUTS));

        assertThat(((CosmosRetryPolicy) retryPolicy).isRetryWriteTimeoutsEnabled())
            .isEqualTo(profile.getBoolean(CosmosRetryPolicyOption.RETRY_WRITE_TIMEOUTS));

        final LoadBalancingPolicy loadBalancingPolicy = context.getLoadBalancingPolicy(profile.getName());

        assertThat(loadBalancingPolicy.getClass()).isEqualTo(CosmosLoadBalancingPolicy.class);

        final Map<UUID, Node> nodes = session.getMetadata().getNodes();
        assertThat(nodes.values().stream().map(node -> node.getEndPoint().resolve())).containsAll(REGIONAL_ENDPOINTS);

        return session;
    }

    /**
     * Tests a retry operation
     */
    private void retry(
        final CosmosRetryPolicy retryPolicy,
        final int retryNumberBegin,
        final int retryNumberEnd,
        final RetryDecision expectedRetryDecision) {

        final CoordinatorException coordinatorException = new OverloadedException(new DefaultNode(
            new DefaultEndPoint(GLOBAL_ENDPOINT),
            (InternalDriverContext) session.getContext()));

        final Request request = SimpleStatement.newInstance("SELECT * FROM retry");

        for (int retryNumber = retryNumberBegin; retryNumber < retryNumberEnd; retryNumber++) {

            final long expectedDuration = 1_000_000 * (retryPolicy.getMaxRetryCount() == -1
                ? FIXED_BACK_OFF_TIME
                : (long) retryNumber * GROWING_BACK_OFF_TIME);

            final long startTime = System.nanoTime();
            final RetryDecision retryDecision = retryPolicy.onErrorResponse(request, coordinatorException, retryNumber);
            final long duration = System.nanoTime() - startTime;

            assertThat(retryDecision).isEqualTo(expectedRetryDecision);
            assertThat((double) duration).isGreaterThan(expectedDuration - 0.01 * expectedDuration);
        }
    }

    // endregion
}
