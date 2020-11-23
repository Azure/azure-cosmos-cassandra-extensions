// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.util.regex.Pattern;

import static java.lang.String.format;

public final class TestCommon {

    private TestCommon() {
        throw new UnsupportedOperationException();
    }

    // region Fields

    static final String GLOBAL_ENDPOINT = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.global-endpoint",
        "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
        "localhost:9042");

    static final Pattern HOSTNAME_AND_PORT = Pattern.compile("^\\s*(?<hostname>.*?):(?<port>\\d+)\\s*$");

    // TODO (DANOBLE) What does the cassandra api return for the local datacenter name when it is hosted by the emulator?
    static final String PASSWORD = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.password",
        "AZURE_COSMOS_CASSANDRA_PASSWORD",
        "");

    static final String USERNAME = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.username",
        "AZURE_COSMOS_CASSANDRA_USERNAME",
        "");

    // endregion

    // region Methods

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    static void createSchema(final CqlSession session, final String keyspaceName, final String tableName) throws InterruptedException {

        session.execute(format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}",
            keyspaceName));

        Thread.sleep(5_000);

        session.execute(format(
            "CREATE TABLE IF NOT EXISTS %s.%s ("
                + "sensor_id uuid,"
                + "date date,"
                + // emulates bucketing by day
                "timestamp timestamp,"
                + "value double,"
                + "PRIMARY KEY ((sensor_id,date),timestamp)"
                + ")",
            keyspaceName,
            tableName));

        Thread.sleep(5_000);
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

        for (final Row row : rows) {
            System.out.printf(format,
                row.getUuid("sensor_id"),
                row.getLocalDate("date"),
                row.getInstant("timestamp"),
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

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    static ResultSet read(
        final CqlSession session,
        final ConsistencyLevel consistencyLevel,
        final String keyspaceName,
        final String tableName) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        final SimpleStatement statement = SimpleStatement.newInstance(format(
            "SELECT sensor_id, date, timestamp, value "
                + "FROM %s.%s "
                + "WHERE "
                + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                + "date = '2018-02-26' AND "
                + "timestamp > '2018-02-26+01:00'",
            keyspaceName,
            tableName)
        ).setConsistencyLevel(consistencyLevel);

        final ResultSet rows = session.execute(statement);

        System.out.println("Read succeeded at " + consistencyLevel);
        return rows;
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    @SuppressWarnings("SameParameterValue")
    static void write(
        final CqlSession session,
        final ConsistencyLevel consistencyLevel,
        final String keyspaceName,
        final String tableName) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED).setConsistencyLevel(consistencyLevel)
            .add(SimpleStatement.newInstance(format("INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)", keyspaceName, tableName)))
            .add(SimpleStatement.newInstance(format("INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:54:27.488+01:00',"
                + "2.47)", keyspaceName, tableName)))
            .add(SimpleStatement.newInstance(format("INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:56:33.739+01:00',"
                + "2.52)", keyspaceName, tableName)));

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
