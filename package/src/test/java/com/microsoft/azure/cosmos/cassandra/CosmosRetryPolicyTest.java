/*
 * The MIT License (MIT)
 *
 * Copyright (c) Microsoft. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.policies.RetryPolicy;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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

    @Test(groups = {"integration", "checkintest"}, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {
        CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT, FIXED_BACK_OFF_TIME, GROWING_BACK_OFF_TIME);

        try {
            this.connect(TestCommon.CONTACT_POINTS, TestCommon.PORT, retryPolicy);

        } catch (Exception error) {
            fail(String.format("connect failed with %s: %s", error.getClass().getCanonicalName(), error));
        }

        try {
            try {
                TestCommon.createSchema(session);

            } catch (Exception error) {
                fail(String.format("createSchema failed: %s", error));
            }
            try {
                TestCommon.write(session, CONSISTENCY_LEVEL);

            } catch (Exception error) {
                fail(String.format("write failed: %s", error));
            }
            try {
                ResultSet rows = TestCommon.read(session, CONSISTENCY_LEVEL);
                TestCommon.display(rows);

            } catch (Exception error) {
                fail(String.format("read failed: %s", error));
            }

        } finally {
            this.close();
        }
    }

    @Test(groups = {"unit", "checkintest"}, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithFixedBackOffTime() {

        CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(-1);
        retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    @Test(groups = {"unit", "checkintest"}, timeOut = TIMEOUT)
    public void canRetryOverloadedExceptionWithGrowingBackOffTime() {

        CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT);
        retry(retryPolicy, 0, MAX_RETRY_COUNT, RetryDecision.Type.RETRY);
    }

    @Test(groups = {"unit", "checkintest"}, timeOut = TIMEOUT)
    public void willRethrowOverloadedExceptionWithGrowingBackOffTime() {

        CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT);
        retry(retryPolicy,MAX_RETRY_COUNT + 1, MAX_RETRY_COUNT + 1, RetryDecision.Type.RETHROW);
    }

    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT = 30000;

    private Cluster cluster;
    private Session session;

    /**
     * Tests a retry operation
     */
    private void retry(CosmosRetryPolicy retryPolicy, int retryNumberBegin, int retryNumberEnd, RetryPolicy.RetryDecision.Type expectedRetryDecisionType) {

        DriverException driverException = new OverloadedException(new InetSocketAddress(TestCommon.CONTACT_POINTS[0], TestCommon.PORT), "retry");
        Statement statement = new SimpleStatement("SELECT * FROM retry");
        ConsistencyLevel consistencyLevel = CONSISTENCY_LEVEL;

        for (int retryNumber = retryNumberBegin; retryNumber < retryNumberEnd; retryNumber++) {

            long expectedDuration = 1000000 * (retryPolicy.getMaxRetryCount() == -1 ? FIXED_BACK_OFF_TIME : retryNumber * GROWING_BACK_OFF_TIME);
            long startTime = System.nanoTime();

            RetryPolicy.RetryDecision retryDecision = retryPolicy.onRequestError(statement, consistencyLevel, driverException, retryNumber);

            long duration = System.nanoTime() - startTime;

            assertThat(retryDecision.getType()).isEqualTo(expectedRetryDecisionType);
            assertThat((double)duration).isGreaterThan(expectedDuration - 0.01 * expectedDuration);
        }
    }

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     *
     * @param contactPoints the contact points to use.
     * @param port          the port to use.
     */
    private void connect(String[] contactPoints, int port, CosmosRetryPolicy retryPolicy) {

        cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).withRetryPolicy(retryPolicy).build();
        System.out.println("Connected to cluster: " + cluster.getClusterName());
        session = cluster.connect();
    }

    /**
     * Closes the session and the cluster.
     */
    private void close() {
        if (session != null) {
            session.close();
            cluster.close();
        }
    }
}
