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
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
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
 * identified by {@link #CONTACT_POINTS} and {@link #PORT}.
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
public class CosmosRetryPolicyExample {

    @Test(groups = {"examples"}, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT, FIXED_BACK_OFF_TIME, GROWING_BACK_OFF_TIME);

        try {
            this.connect(CONTACT_POINTS, PORT, retryPolicy);

        } catch (Exception error) {
            fail(String.format("connect failed with %s: %s", error.getClass().getCanonicalName(), error));
        }

        try {
            try {
                this.createSchema();

            } catch (Exception error) {
                fail(String.format("createSchema failed: %s", error));
            }
            try {
                this.write(CONSISTENCY_LEVEL);

            } catch (Exception error) {
                fail(String.format("write failed: %s", error));
            }
            try {
                ResultSet rows = this.read(CONSISTENCY_LEVEL);
                this.display(rows);

            } catch (Exception error) {
                fail(String.format("read failed: %s", error));
            }

        } finally {
            this.close();
        }
    }

    // Privates

    private static final String[] CONTACT_POINTS;
    static {

        String value = System.getProperty("COSMOS_HOSTNAME");

        if (value == null) {
            value = System.getenv("COSMOS_HOSTNAME");
        }

        if (value == null) {
            value = "localhost";
        }

        CONTACT_POINTS = new String[] {value};
    }

    private static final int PORT;
    static {

        String value = System.getProperty("COSMOS_PORT");

        if (value == null) {
            value = System.getenv("COSMOS_PORT");
        }

        if (value == null) {
            value = "10350";
        }

        PORT = Short.parseShort(value);
    };

    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT = 30000;

    private Cluster cluster;
    private Session session;

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
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    private void createSchema() {

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS downgrading WITH replication "
                        + "= {'class':'SimpleStrategy', 'replication_factor':3}");

        session.execute(
                "CREATE TABLE IF NOT EXISTS downgrading.sensor_data ("
                        + "sensor_id uuid,"
                        + "date date,"
                        + // emulates bucketing by day
                        "timestamp timestamp,"
                        + "value double,"
                        + "PRIMARY KEY ((sensor_id,date),timestamp)"
                        + ")");
    }

    /**
     * Tests a retry operation
     */
    private void retry(CosmosRetryPolicy retryPolicy, int retryNumberBegin, int retryNumberEnd, RetryDecision.Type expectedRetryDecisionType) {

        DriverException driverException = new OverloadedException(new InetSocketAddress(CONTACT_POINTS[0], PORT), "retry");
        Statement statement = new SimpleStatement("SELECT * FROM retry");
        ConsistencyLevel consistencyLevel = CONSISTENCY_LEVEL;

        for (int retryNumber = retryNumberBegin; retryNumber < retryNumberEnd; retryNumber++) {

            long expectedDuration = 1000000 * (retryPolicy.getMaxRetryCount() == -1 ? FIXED_BACK_OFF_TIME : retryNumber * GROWING_BACK_OFF_TIME);
            long startTime = System.nanoTime();

            RetryDecision retryDecision = retryPolicy.onRequestError(statement, consistencyLevel, driverException, retryNumber);

            long duration = System.nanoTime() - startTime;

            assertThat(retryDecision.getType()).isEqualTo(expectedRetryDecisionType);
            assertThat(duration).isGreaterThan(expectedDuration);
        }
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private void write(ConsistencyLevel consistencyLevel) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        BatchStatement batch = new BatchStatement(UNLOGGED);

        batch.add(
                new SimpleStatement(
                        "INSERT INTO downgrading.sensor_data "
                                + "(sensor_id, date, timestamp, value) "
                                + "VALUES ("
                                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                                + "'2018-02-26',"
                                + "'2018-02-26T13:53:46.345+01:00',"
                                + "2.34)"));

        batch.add(
                new SimpleStatement(
                        "INSERT INTO downgrading.sensor_data "
                                + "(sensor_id, date, timestamp, value) "
                                + "VALUES ("
                                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                                + "'2018-02-26',"
                                + "'2018-02-26T13:54:27.488+01:00',"
                                + "2.47)"));

        batch.add(
                new SimpleStatement(
                        "INSERT INTO downgrading.sensor_data "
                                + "(sensor_id, date, timestamp, value) "
                                + "VALUES ("
                                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                                + "'2018-02-26',"
                                + "'2018-02-26T13:56:33.739+01:00',"
                                + "2.52)"));

        batch.setConsistencyLevel(consistencyLevel);

        session.execute(batch);
        System.out.println("Write succeeded at " + consistencyLevel);
    }

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private ResultSet read(ConsistencyLevel consistencyLevel) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        Statement statement =
                new SimpleStatement(
                        "SELECT sensor_id, date, timestamp, value "
                                + "FROM downgrading.sensor_data "
                                + "WHERE "
                                + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                                + "date = '2018-02-26' AND "
                                + "timestamp > '2018-02-26+01:00'")
                        .setConsistencyLevel(consistencyLevel);

        ResultSet rows = session.execute(statement);
        System.out.println("Read succeeded at " + consistencyLevel);
        return rows;
    }

    /**
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    private void display(ResultSet rows) {

        final int width1 = 38;
        final int width2 = 12;
        final int width3 = 30;
        final int width4 = 21;

        String format =
                "%-" + width1 + "s" + "%-" + width2 + "s" + "%-" + width3 + "s" + "%-" + width4 + "s"
                        + "%n";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        // headings
        System.out.printf(format, "sensor_id", "date", "timestamp", "value");

        // separators
        drawLine(width1, width2, width3, width4);

        // data
        for (Row row : rows) {

            System.out.printf(
                    format,
                    row.getUUID("sensor_id"),
                    row.getDate("date"),
                    sdf.format(row.getTimestamp("timestamp")),
                    row.getDouble("value"));
        }
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

    /**
     * Draws a line to isolate headings from rows.
     *
     * @param widths the column widths.
     */
    private static void drawLine(int... widths) {
        for (int width : widths) {
            for (int i = 1; i < width; i++) {
                System.out.print('-');
            }
            System.out.print('+');
        }
        System.out.println();
    }
}
