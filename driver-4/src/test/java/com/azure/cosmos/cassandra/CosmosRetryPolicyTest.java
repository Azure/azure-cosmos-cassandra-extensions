// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
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

import java.util.Map;
import java.util.UUID;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.LOCAL_DATACENTER;
import static com.azure.cosmos.cassandra.TestCommon.display;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.junit.jupiter.api.Assertions.fail;

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

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private static final int FIXED_BACK_OFF_TIME = CosmosRetryPolicyOption.FIXED_BACKOFF_TIME.getDefaultValue(Integer.class);
    private static final int GROWING_BACK_OFF_TIME = CosmosRetryPolicyOption.GROWING_BACKOFF_TIME.getDefaultValue(Integer.class);
    private static final String KEYSPACE_NAME = TestCommon.uniqueName("downgrading");
    private static final int MAX_RETRIES = CosmosRetryPolicyOption.MAX_RETRIES.getDefaultValue(Integer.class);
    private static final String TABLE_NAME = "sensor_data";
    private static final int TIMEOUT_IN_SECONDS = 30;

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

        assertThatCode(() ->
            TestCommon.createSchema(session, KEYSPACE_NAME, TABLE_NAME)
        ).doesNotThrowAnyException();

        assertThatCode(() ->
            TestCommon.write(session, CONSISTENCY_LEVEL, KEYSPACE_NAME, TABLE_NAME)
        ).doesNotThrowAnyException();

        assertThatCode(() -> {
            final ResultSet rows = TestCommon.read(session, CONSISTENCY_LEVEL, KEYSPACE_NAME, TABLE_NAME);
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
        // TODO (DANOBLE) Is the expected retry decision RetryDecision.RETRY_SAME or something else?
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
        // TODO (DANOBLE) Is the expected retry decision RetryDecision.RETRY_SAME or something else?
        this.retry(retryPolicy, 0, MAX_RETRIES, RetryDecision.RETRY_SAME);
    }

    /**
     * Closes the {@link #session} for testing {@link CosmosRetryPolicy} after dropping {@link #KEYSPACE_NAME}, if it
     * exists.
     */
    @AfterAll
    @Timeout(TIMEOUT_IN_SECONDS)
    public static void cleanUp() {
        if (session != null && !session.isClosed()) {
            try {
                session.execute(format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_NAME));
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
    public static void connect() {

        session = checkState(CqlSession.builder().withConfigLoader(
            DriverConfigLoader.programmaticBuilder()
                .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DefaultLoadBalancingPolicy.class)
                .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, LOCAL_DATACENTER)
                .build())
            .build());
    }

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

        assertThat(((CosmosRetryPolicy) retryPolicy).getFixedBackoffTimeInMillis())
            .isEqualTo(profile.getInt(CosmosRetryPolicyOption.FIXED_BACKOFF_TIME));

        assertThat(((CosmosRetryPolicy) retryPolicy).getGrowingBackoffTimeInMillis())
            .isEqualTo(profile.getInt(CosmosRetryPolicyOption.GROWING_BACKOFF_TIME));

        assertThat(((CosmosRetryPolicy) retryPolicy).getMaxRetryCount())
            .isEqualTo(profile.getInt(CosmosRetryPolicyOption.MAX_RETRIES));

        final LoadBalancingPolicy loadBalancingPolicy = context.getLoadBalancingPolicy(profile.getName());

        assertThat(loadBalancingPolicy.getClass()).isEqualTo(DefaultLoadBalancingPolicy.class);

        final Map<UUID, Node> nodes = session.getMetadata().getNodes();
//        assertThat(nodes.values().stream().map(node -> node.getEndPoint().resolve())).containsAll(NODES);

        LOG.info("[{}] connected to {} with {} and {}",
            session.getName(),
            nodes,
            retryPolicy,
            loadBalancingPolicy);

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
