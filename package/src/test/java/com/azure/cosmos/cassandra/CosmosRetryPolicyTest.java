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
import java.util.ArrayList;
import java.util.Collection;

import static com.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.azure.cosmos.cassandra.TestCommon.LOCAL_DATACENTER;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.PORT;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.display;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
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
public class CosmosRetryPolicyTest implements AutoCloseable {

    // region Fields

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private static final int FIXED_BACK_OFF_TIME = 5_000;
    private static final int GROWING_BACK_OFF_TIME = 1_000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT = 30_0000;

    private final String keyspaceName = "downgrading";
    private final String tableName = "sensor_data";
    private CqlSession session;

    // endregion

    // region Methods

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    @SuppressWarnings("CatchMayIgnoreException")
    public void canIntegrateWithCosmos() {

        assertThatCode(() ->
            TestCommon.createSchema(session, this.keyspaceName, this.tableName)
        ).doesNotThrowAnyException();

        assertThatCode(() ->
            TestCommon.write(session, CONSISTENCY_LEVEL, this.keyspaceName, this.tableName)
        ).doesNotThrowAnyException();

        assertThatCode(() -> {
            ResultSet rows = TestCommon.read(session, CONSISTENCY_LEVEL, this.keyspaceName, this.tableName);
            display(rows);
        }).doesNotThrowAnyException();
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void canRetryOnConnectionException() {

        final CoordinatorException coordinatorException = new ServerError(
            new DefaultNode(
                new DefaultEndPoint(new InetSocketAddress(CONTACT_POINTS[0], PORT)),
                (InternalDriverContext) this.session.getContext()), "canRetryOnConnectionException");

        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT);
        final Request request = SimpleStatement.newInstance("SELECT * FROM retry");

        for (int retryNumber = 0; retryNumber < MAX_RETRY_COUNT; retryNumber++) {
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
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.RETRY_SAME);
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {
        final CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT);
        // TODO (DANOBLE) Is the expected retry decision RetryDecision.RETRY_SAME or something else?
        this.retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.RETRY_SAME);
    }

    /**
     * Closes the session, if it's been instantiated.
     */
    @AfterClass
    public void close() {
        if (this.session != null) {
            this.session.close();
        }
    }

    @BeforeClass
    public void connect() {
        this.session = this.connect(CONTACT_POINTS, PORT, USERNAME, PASSWORD, LOCAL_DATACENTER);
    }

    @Test(groups = { "unit", "checkin" }, timeOut = TIMEOUT)
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {
        CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT);
        this.retry(retryPolicy, MAX_RETRY_COUNT + 1, MAX_RETRY_COUNT + 1, RetryDecision.RETHROW);
    }

    // endregion

    // region Privates

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     * @param contactPoints the contact points to use.
     * @param port          the port to use.
     */
    private CqlSession connect(
        String[] contactPoints, int port, String username, String password, String localDatacenter) {

        final Collection<EndPoint> endpoints = new ArrayList<>(contactPoints.length);

        for (String contactPoint : contactPoints) {

            final int index = contactPoint.lastIndexOf(':');

            final InetSocketAddress address = new InetSocketAddress(
                index < 0 ? contactPoint : contactPoint.substring(0, index),
                port);

            endpoints.add(new DefaultEndPoint(address));
        }

        this.session = CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DefaultLoadBalancingPolicy.class)
                .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, CosmosRetryPolicy.class)
                .build())
            .withAuthCredentials(username, password)
            .withLocalDatacenter(localDatacenter)
            .addContactEndPoints(endpoints)
            .build();

        System.out.println("Connected to session: " + this.session.getName());
        return this.session;
    }

    /**
     * Tests a retry operation
     */
    private void retry(
        CosmosRetryPolicy retryPolicy, int retryNumberBegin, int retryNumberEnd, RetryDecision expectedRetryDecision) {

        final CoordinatorException coordinatorException = new OverloadedException(
            new DefaultNode(
                new DefaultEndPoint(new InetSocketAddress(CONTACT_POINTS[0], PORT)),
                (InternalDriverContext) this.session.getContext()));

        final Request request = SimpleStatement.newInstance("SELECT * FROM retry");

        for (int retryNumber = retryNumberBegin; retryNumber < retryNumberEnd; retryNumber++) {

            final long expectedDuration = 1000000 * (retryPolicy.getMaxRetryCount() == -1
                ? FIXED_BACK_OFF_TIME
                : (long) retryNumber * GROWING_BACK_OFF_TIME);

            final long startTime = System.nanoTime();

            RetryDecision retryDecision = retryPolicy.onErrorResponse(request, coordinatorException, retryNumber);

            final long duration = System.nanoTime() - startTime;

            assertThat(retryDecision).isEqualTo(expectedRetryDecision);
            assertThat((double) duration).isGreaterThan(expectedDuration - 0.01 * expectedDuration);
        }
    }

    // endregion
}
