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

import java.text.SimpleDateFormat;
import java.util.UUID;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.fail;

public final class TestCommon {

    private TestCommon() {
        throw new UnsupportedOperationException();
    }

    // region Fields

    static final String[] CONTACT_POINTS;

    static final String GLOBAL_ENDPOINT = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.global-endpoint",
        "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
        "localhost:9042");
    static final String PASSWORD = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.password",
        "AZURE_COSMOS_CASSANDRA_PASSWORD",
        "");

    static final int PORT;

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
            fail("expected integer port number in range [0, 65535], not " + port);
        }

        CONTACT_POINTS = new String[] { hostname };
        PORT = value;
    }

    // endregion

    // region Methods

    /**
     * Closes the given {@link Session} and its associated {@link Cluster cluster} after dropping the given
     * {@code keyspaceName}.
     *
     * @param session      Session to be closed.
     * @param keyspaceName Name of keyspace to be dropped before closing {@code session} and its associated
     *                     {@link Cluster cluster}.
     */
    static void cleanUp(final Session session, final String keyspaceName) {
        if (session != null && !session.isClosed()) {
            try {
                session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
            } finally {
                session.close();
                session.getCluster().close();
            }
        }
    }

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    static void createSchema(
        final Session session, final String keyspaceName, final String tableName) throws InterruptedException {

        session.execute(format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':3}",
            keyspaceName));

        Thread.sleep(5000);

        session.execute(format(
            "CREATE TABLE IF NOT EXISTS %s.%s ("
                + "sensor_id uuid,"
                + "date date,"
                + "timestamp timestamp,"  // emulates bucketing by day
                + "value double,"
                + "PRIMARY KEY ((sensor_id,date),timestamp)"
                + ")",
            keyspaceName,
            tableName));

        Thread.sleep(5000);
    }

    /**
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    static void display(final ResultSet rows) {

        final int width1 = 38;
        final int width2 = 12;
        final int width3 = 30;
        final int width4 = 21;

        final String format = "%-" + width1 + "s%-" + width2 + "s%-" + width3 + "s%-" + width4 + "s%n";
        System.out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        System.out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        for (final Row row : rows) {
            System.out.printf(format,
                row.getUUID("sensor_id"),
                row.getDate("date"),
                sdf.format(row.getTimestamp("timestamp")),
                row.getDouble("value"));
        }
    }

    static String getPropertyOrEnvironmentVariable(final String property, final String variable, final String defaultValue) {

        String value = System.getProperty(property);

        if (value == null) {
            value = System.getenv(variable);
        }

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    static ResultSet read(final Session session, final String keyspaceName, final String tableName) {
        return read(session, ConsistencyLevel.ONE, keyspaceName, tableName);
    }

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    static ResultSet read(
        final Session session,
        final ConsistencyLevel consistencyLevel,
        final String keyspaceName,
        final String tableName) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        final Statement statement =
            new SimpleStatement(format(
                "SELECT sensor_id, date, timestamp, value "
                    + "FROM %s.%s "
                    + "WHERE "
                    + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                    + "date = '2018-02-26' AND "
                    + "timestamp > '2018-02-26+01:00'", keyspaceName, tableName))
                .setConsistencyLevel(consistencyLevel);

        final ResultSet rows = session.execute(statement);
        System.out.println("Read succeeded at " + consistencyLevel);
        return rows;
    }

    static String uniqueName(final String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    static void write(final Session session, final String keyspaceName, final String tableName) {
        write(session, ConsistencyLevel.ONE, keyspaceName, tableName);
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    static void write(
        final Session session,
        final ConsistencyLevel consistencyLevel,
        final String keyspaceName,
        final String tableName) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = new BatchStatement(UNLOGGED);

        batch.add(new SimpleStatement(format(
            "INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)",
            keyspaceName,
            tableName)));

        batch.add(new SimpleStatement(format(
            "INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:54:27.488+01:00',"
                + "2.47)",
            keyspaceName,
            tableName)));

        batch.add(new SimpleStatement(format(
            "INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:56:33.739+01:00',"
                + "2.52)",
            keyspaceName,
            tableName)));

        batch.setConsistencyLevel(consistencyLevel);

        session.execute(batch);
        System.out.println("Write succeeded at " + consistencyLevel);
    }

    // endregion

    // region Privates

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

    // endregion
}
