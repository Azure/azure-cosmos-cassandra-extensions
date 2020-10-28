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

import java.text.SimpleDateFormat;

import static java.lang.String.format;

public final class TestCommon {

    private TestCommon() {
        // this is a static class
    }

    // region Fields

    static final String[] CONTACT_POINTS = { getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.contactPoint",
        "CASSANDRA_CONTACT_POINT",
        "localhost") };

    static final String CREDENTIALS_PASSWORD = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.credentials.password",
        "CASSANDRA_CREDENTIALS_PASSWORD",
        ""
    );

    static final String CREDENTIALS_USERNAME = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.credentials.username",
        "CASSANDRA_CREDENTIALS_USERNAME",
        "");

    static final int PORT = Short.parseShort(getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.port",
        "CASSANDRA_PORT",
        "9042"));


    // endregion

    // region Methods

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    static void createSchema(CqlSession session, String keyspaceName, String tableName) throws InterruptedException {

        session.execute(format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}",
            keyspaceName));

        Thread.sleep(5000);

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

        Thread.sleep(5000);
    }

    /**
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    static void display(ResultSet rows) {

        final int width1 = 38;
        final int width2 = 12;
        final int width3 = 30;
        final int width4 = 21;

        final String format = "%-" + width1 + "s%-" + width2 + "s%-" + width3 + "s%-" + width4 + "s%n";
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        // headings
        System.out.printf(format, "sensor_id", "date", "timestamp", "value");

        // separators
        drawLine(width1, width2, width3, width4);

        // data

        for (Row row : rows) {
            System.out.printf(format,
                row.getUuid("sensor_id"),
                row.getLocalDate("date"),
                sdf.format(row.getInstant("timestamp")),
                row.getDouble("value"));
        }
    }

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    static ResultSet read(
        CqlSession session,
        ConsistencyLevel consistencyLevel,
        String keyspaceName,
        String tableName) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        SimpleStatement statement = SimpleStatement.newInstance(format(
            "SELECT sensor_id, date, timestamp, value "
                + "FROM %s.%s "
                + "WHERE "
                + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                + "date = '2018-02-26' AND "
                + "timestamp > '2018-02-26+01:00'",
            keyspaceName,
            tableName)
        ).setConsistencyLevel(consistencyLevel);

        ResultSet rows = session.execute(statement);

        System.out.println("Read succeeded at " + consistencyLevel);
        return rows;
    }

    static ResultSet read(CqlSession session, String keyspaceName, String tableName) {
        return read(session, ConsistencyLevel.ONE, keyspaceName, tableName);
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    static void write(CqlSession session, ConsistencyLevel consistencyLevel, String keyspaceName, String tableName) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED).setConsistencyLevel(consistencyLevel);

        batch.add(SimpleStatement.newInstance(format(
            "INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)", keyspaceName, tableName)));

        batch.add(SimpleStatement.newInstance(format(
            "INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:54:27.488+01:00',"
                + "2.47)", keyspaceName, tableName)));

        batch.add(SimpleStatement.newInstance(format(
            "INSERT INTO %s.%s "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:56:33.739+01:00',"
                + "2.52)", keyspaceName, tableName)));

        session.execute(batch);
        System.out.println("Write succeeded at " + consistencyLevel);
    }

    static void write(CqlSession session, String keyspaceName, String tableName) {
        write(session, ConsistencyLevel.ONE, keyspaceName, tableName);
    }

    // endregion

    // region Privates

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

    private static String getPropertyOrEnvironmentVariable(
        String property,
        String variable,
        String defaultValue) {

        String value = System.getProperty(property);

        if (value == null) {
            value = System.getenv(variable);
        }

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    // endregion
}
