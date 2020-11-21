// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
public class CosmosCassandraExtensionsExample {

    // region Fields

    static final String GLOBAL_ENDPOINT = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.global-endpoint",
        "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
        "localhost:9042");

    static final String PASSWORD = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.password",
        "AZURE_COSMOS_CASSANDRA_PASSWORD",
        "");

    static final String READ_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.read-datacenter",
        "AZURE_COSMOS_CASSANDRA_READ_DATACENTER",
        "");

    static final String USERNAME = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.username",
        "AZURE_COSMOS_CASSANDRA_USERNAME",
        "");

    static final String WRITE_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.write-datacenter",
        "AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER",
        "");

    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;
    private static final String[] CONTACT_POINTS;
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final int PORT;
    private static final int TIMEOUT = 30000;

    static {

        final int index = GLOBAL_ENDPOINT.lastIndexOf(':');
        assertThat(index).isGreaterThan(0);

        final String hostname = GLOBAL_ENDPOINT.substring(0, index);
        final String port = GLOBAL_ENDPOINT.substring(index + 1);
        int value = -1;

        try {
            value = Integer.parseUnsignedInt(port);
            assertThat(value).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(65535);
        } catch (final Throwable error) {
            AssertJUnit.fail("expected integer port number in range [0, 65535], not " + port);
        }

        CONTACT_POINTS = new String[] { hostname };
        PORT = value;
    }

    private Session session;

    // endregion

    // region Methods

    @Test(groups = { "examples" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        assertThatCode(this::connect).doesNotThrowAnyException();

        try {
            assertThatCode(this::createSchema).doesNotThrowAnyException();

            assertThatCode(() ->
                this.write(CONSISTENCY_LEVEL)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = this.read(CONSISTENCY_LEVEL);
                this.display(rows);
            }).doesNotThrowAnyException();

        } finally {
            this.close();
        }
    }

    // endregion

    // Privates

    /**
     * Closes the session and the cluster.
     */
    private void close() {
        if (this.session != null && !this.session.isClosed()) {
            this.session.close();
            this.session.getCluster().close();
        }
    }

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     */
    private void connect() {

        final Cluster cluster = Cluster.builder()
            .withLoadBalancingPolicy(CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(GLOBAL_ENDPOINT)
                .withReadDC(READ_DATACENTER)
                .withWriteDC(WRITE_DATACENTER)
                .build())
            .withRetryPolicy(CosmosRetryPolicy.builder()
                .withFixedBackOffTimeInMillis(FIXED_BACK_OFF_TIME)
                .withGrowingBackOffTimeInMillis(GROWING_BACK_OFF_TIME)
                .withMaxRetryCount(MAX_RETRY_COUNT)
                .build())
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoints(CONTACT_POINTS)
            .withPort(PORT)
            .withSSL()
            .build();

        try {
            this.session = cluster.connect();
        } catch (final Throwable error) {
            cluster.close();
            throw error;
        }

        System.out.println("Connected to " + cluster.getClusterName());
    }

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    private void createSchema() {

        this.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS downgrading WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':3}");

        this.session.execute(
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
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    private void display(final ResultSet rows) {

        final int width1 = 38;
        final int width2 = 12;
        final int width3 = 30;
        final int width4 = 21;

        final String format = "%-" + width1 + "s" + "%-" + width2 + "s" + "%-" + width3 + "s" + "%-" + width4 + "s" + "%n";

        System.out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        for (final Row row : rows) {
            System.out.printf(
                format,
                row.getUUID("sensor_id"),
                row.getDate("date"),
                sdf.format(row.getTimestamp("timestamp")),
                row.getDouble("value"));
        }
    }

    /**
     * Draws a line to isolate headings from rows.
     *
     * @param widths the column widths.
     */
    private static void drawLine(final int... widths) {
        for (final int width : widths) {
            for (int i = 1; i < width; i++) {
                System.out.print('-');
            }
            System.out.print('+');
        }
        System.out.println();
    }

    private static String getPropertyOrEnvironmentVariable(
        final String property, final String variable, final String defaultValue) {

        String value = System.getProperty(property);

        if (value == null) {
            value = System.getenv(variable);
        }

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private ResultSet read(final ConsistencyLevel consistencyLevel) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        final Statement statement =
            new SimpleStatement(
                "SELECT sensor_id, date, timestamp, value "
                    + "FROM downgrading.sensor_data "
                    + "WHERE "
                    + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                    + "date = '2018-02-26' AND "
                    + "timestamp > '2018-02-26+01:00'")
                .setConsistencyLevel(consistencyLevel);

        final ResultSet rows = this.session.execute(statement);
        System.out.println("Read succeeded at " + consistencyLevel);
        return rows;
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private void write(final ConsistencyLevel consistencyLevel) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = new BatchStatement(UNLOGGED)
            .add(new SimpleStatement(
                "INSERT INTO downgrading.sensor_data "
                    + "(sensor_id, date, timestamp, value) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "'2018-02-26',"
                    + "'2018-02-26T13:53:46.345+01:00',"
                    + "2.34)"))
            .add(new SimpleStatement(
                "INSERT INTO downgrading.sensor_data "
                    + "(sensor_id, date, timestamp, value) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "'2018-02-26',"
                    + "'2018-02-26T13:54:27.488+01:00',"
                    + "2.47)"))
            .add(new SimpleStatement(
                "INSERT INTO downgrading.sensor_data "
                    + "(sensor_id, date, timestamp, value) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "'2018-02-26',"
                    + "'2018-02-26T13:56:33.739+01:00',"
                    + "2.52)"));

        batch.setConsistencyLevel(consistencyLevel);
        this.session.execute(batch);

        System.out.println("Write succeeded at " + consistencyLevel);
    }

    // endregion
}
