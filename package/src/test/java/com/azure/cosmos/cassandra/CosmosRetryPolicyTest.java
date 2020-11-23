// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.regex.Matcher;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.HOSTNAME_AND_PORT;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.display;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/**
 * This test illustrates use of the {@link CosmosRetryPolicy} class.
 * <p>
 * Preconditions:
 * <ul>
 * <li>An Apache Cassandra cluster is running and accessible through the contacts points
 * identified by #CONTACT_POINTS and #PORT.
 * </ul>
 * <p>
 * Side effects:
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

    static final String LOCAL_DATACENTER = TestCommon.getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.local-datacenter",
        "AZURE_COSMOS_CASSANDRA_LOCAL_DATACENTER",
        "localhost");

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private static final int FIXED_BACK_OFF_TIME = CosmosRetryPolicy.Option.FIXED_BACKOFF_TIME.getDefaultValue();
    private static final int GROWING_BACK_OFF_TIME = CosmosRetryPolicy.Option.GROWING_BACKOFF_TIME.getDefaultValue();
    private static final String KEYSPACE_NAME = "downgrading_" + UUID.randomUUID().toString().replace("-", "");
    private static final int MAX_RETRIES = CosmosRetryPolicy.Option.MAX_RETRIES.getDefaultValue();
    private static final String TABLE_NAME = "sensor_data";
    private static final int TIMEOUT = 30_0000;

    private CqlSession session;

    // endregion

    // region Methods

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        assertThatCode(() ->
            TestCommon.createSchema(this.session, KEYSPACE_NAME, TABLE_NAME)
        ).doesNotThrowAnyException();

        assertThatCode(() ->
            TestCommon.write(this.session, CONSISTENCY_LEVEL, KEYSPACE_NAME, TABLE_NAME)
        ).doesNotThrowAnyException();

        assertThatCode(() -> {
            final ResultSet rows = TestCommon.read(this.session, CONSISTENCY_LEVEL, KEYSPACE_NAME, TABLE_NAME);
            display(rows);
        }).doesNotThrowAnyException();
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void canRetryOnConnectionException() {

        final Matcher address = HOSTNAME_AND_PORT.matcher(GLOBAL_ENDPOINT);
        assertThat(address.matches()).isTrue();

        final CoordinatorException coordinatorException = new ServerError(new DefaultNode(
            new DefaultEndPoint(
                new InetSocketAddress(
                    address.group("hostname"),
                    Integer.parseUnsignedInt(address.group("port")))),
            (InternalDriverContext) this.session.getContext()), "canRetryOnConnectionException");

        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRIES);
        final Request request = SimpleStatement.newInstance("SELECT * FROM retry");

        for (int retryNumber = 0; retryNumber < MAX_RETRIES; retryNumber++) {
            final RetryDecision retryDecision = retryPolicy.onErrorResponse(request, coordinatorException, retryNumber);
            // TODO (DANOBLE) Is this the expected return value or should it be RETRY_NEXT?
            //  Should we cycle through nodes in response to an error or retry on the same node?
            assertThat(retryDecision).isEqualTo(RetryDecision.RETRY_SAME);
        }
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithFixedBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(-1);
        // TODO (DANOBLE) Is the expected retry decision RetryDecision.RETRY_SAME or something else?
        this.retry(retryPolicy, 0, MAX_RETRIES, RetryDecision.RETRY_SAME);
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRIES);
        // TODO (DANOBLE) Is the expected retry decision RetryDecision.RETRY_SAME or something else?
        this.retry(retryPolicy, 0, MAX_RETRIES, RetryDecision.RETRY_SAME);
    }

    /**
     * Closes the session, if it's been instantiated.
     */
    @AfterClass
    public void cleanUp() {
        if (this.session != null && !this.session.isClosed()) {
            try {
                this.session.execute(format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_NAME));
            } finally {
                this.session.close();
            }
        }
    }

    @BeforeClass
    public void connect() {
        this.session = this.connect(GLOBAL_ENDPOINT, USERNAME, PASSWORD, LOCAL_DATACENTER);
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRIES);
        this.retry(retryPolicy, MAX_RETRIES + 1, MAX_RETRIES + 1, RetryDecision.RETHROW);
    }

    // endregion

    // region Privates

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     * @param globalEndPoint the contact points to use.
     * @param username the username for authenticating.
     * @param password the password for authenticating.
     */
    @SuppressWarnings("SameParameterValue")
    private CqlSession connect(
        final String globalEndPoint, final String username, final String password, final String localDatacenter) {

        final Matcher address = HOSTNAME_AND_PORT.matcher(globalEndPoint);
        assertThat(address.matches()).isTrue();

        final Collection<EndPoint> endPoints = Collections.singletonList(new DefaultEndPoint(new InetSocketAddress(
            address.group("hostname"),
            Integer.parseUnsignedInt(address.group("port")))));

        this.session = CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DefaultLoadBalancingPolicy.class)
                .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, CosmosRetryPolicy.class)
                .build())
            .withAuthCredentials(username, password)
            .withLocalDatacenter(localDatacenter)
            .addContactEndPoints(endPoints)
            .build();

        return this.session;
    }

    /**
     * Tests a retry operation
     */
    private void retry(
        final CosmosRetryPolicy retryPolicy,
        final int retryNumberBegin,
        final int retryNumberEnd,
        final RetryDecision expectedRetryDecision) {

        final Matcher address = HOSTNAME_AND_PORT.matcher(GLOBAL_ENDPOINT);
        assertThat(address.matches()).isTrue();

        final CoordinatorException coordinatorException = new OverloadedException(new DefaultNode(
            new DefaultEndPoint(new InetSocketAddress(
                address.group("hostname"),
                Integer.parseInt(address.group("port")))),
            (InternalDriverContext) this.session.getContext()));

        final Request request = SimpleStatement.newInstance("SELECT * FROM retry");

        for (int retryNumber = retryNumberBegin; retryNumber < retryNumberEnd; retryNumber++) {

            final long expectedDuration = 1000000 * (retryPolicy.getMaxRetryCount() == -1
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