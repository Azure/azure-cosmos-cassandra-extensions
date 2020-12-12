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
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A utility class that implements common static methods useful for writing tests.
 */
public final class TestCommon {

    static final List<String> CONTACT_POINTS = Arrays.asList(getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.contact-point",
        "AZURE_COSMOS_CASSANDRA_CONTACT_POINT",
        "localhost:10350").split("\\s*,\\s*"));

    // region Fields
    static final String GLOBAL_ENDPOINT = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.global-endpoint",
        "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
        "localhost:10350");
    // TODO (DANOBLE) What does the cassandra api return for the local datacenter name when it is hosted by the
    //  emulator?
    static final String PASSWORD = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.password",
        "AZURE_COSMOS_CASSANDRA_PASSWORD",
        "");
    static final String USERNAME = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.username",
        "AZURE_COSMOS_CASSANDRA_USERNAME",
        "");
    private static final Pattern HOSTNAME_AND_PORT = Pattern.compile("^\\s*(?<hostname>.*?):(?<port>\\d+)\\s*$");

    private TestCommon() {
        throw new UnsupportedOperationException();
    }

    // endregion

    // region Methods

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     *
     * @param session      the session for executing requests.
     * @param keyspaceName name of the keyspace to query.
     * @param tableName    name of the table to query.
     */
    static void createSchema(final CqlSession session, final String keyspaceName, final String tableName)
        throws InterruptedException {

        session.execute(format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}",
            keyspaceName));

        Thread.sleep(5_000);

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

        Thread.sleep(5_000);
    }

    /**
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    static void display(@NonNull final ResultSet rows) {

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

    /**
     * Get the value of the specified system {@code property} or--if it is unset--environment {@code variable}.
     * <p>
     * If neither {@code property} or {@code variable} is set, {@code defaultValue} is returned.
     *
     * @param property     a system property name.
     * @param variable     an environment variable name.
     * @param defaultValue the default value--which may be {@code null}--to be used if neither {@code property} or
     *                     {@code variable} is set.
     *
     * @return The value of the specified {@code property}, the value of the specified environment {@code variable}, or
     * {@code defaultValue}.
     */
    static String getPropertyOrEnvironmentVariable(
        @NonNull final String property, @NonNull final String variable, final String defaultValue) {

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
     * Returns a {@link Matcher Matcher} that matches the {@code hostname} and {@code port} parts of a network socket
     * address.
     * <p>
     * Retrieve the {@code hostname} and {@code port} from the returned {@link Matcher Matcher} like this:
     * <pre>{@code
     * String hostname = matcher.group("hostname")
     * String port = matcher.group("port")
     * }</pre>
     *
     * @param socketAddress a socket address of the form <i>&lt;hostname&gt;</i><b>:</b><i>&lt;port&gt;</i>
     *
     * @return a {@link Matcher Matcher} that matches the {@code hostname} and {@code port} parts of a network socket
     * address.
     */
    @NonNull
    static Matcher matchSocketAddress(final String socketAddress) {
        final Matcher matcher = HOSTNAME_AND_PORT.matcher(socketAddress);
        assertThat(matcher.matches()).isTrue();
        return matcher;
    }
    /**
     * Queries data, retrying if necessary with a downgraded consistency level.
     *
     * @param session          the session for executing requests.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     */
    @SuppressWarnings("SameParameterValue")
    @NonNull
    static ResultSet read(
        @NonNull final CqlSession session,
        final ConsistencyLevel consistencyLevel,
        @NonNull final String keyspaceName,
        @NonNull final String tableName) {

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
     * Returns a unique name composed of a {@code prefix} string and a {@linkplain UUID#randomUUID random UUID}.
     * <p>
     * Hyphens are removed from the generated {@link UUID} before it is joined to the {@code prefix} with an underscore.
     *
     * @param prefix a string that starts the unique name.
     *
     * @return a unique name of the form <i>&lt;prefix&gt;</i><b><code>_</code></b><i>&lt;random-uuid&gt;</i>.
     */
    @NonNull
    static String uniqueName(@NonNull final String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    // endregion

    // region Privates

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param session          the session for executing requests.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     */
    @SuppressWarnings("SameParameterValue")
    static void write(
        @NonNull final CqlSession session,
        final ConsistencyLevel consistencyLevel,
        @NonNull final String keyspaceName,
        @NonNull final String tableName) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED)
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
                + "2.52)", keyspaceName, tableName)))
            .setConsistencyLevel(consistencyLevel);

        session.execute(batch);
        System.out.println("Write succeeded at " + consistencyLevel);
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

    // endregion
}
