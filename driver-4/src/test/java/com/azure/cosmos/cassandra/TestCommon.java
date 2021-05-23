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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.System.out;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A utility class that implements common static methods useful for writing tests.
 */
public final class TestCommon {

    private TestCommon() {
        throw new UnsupportedOperationException();
    }

    // region Fields

    private static final Pattern HOSTNAME_AND_PORT = Pattern.compile("^\\s*(?<hostname>.*?):(?<port>\\d+)\\s*$");

    static final InetSocketAddress GLOBAL_ENDPOINT;
    static final String LOCAL_DATACENTER;
    static final List<String> PREFERRED_REGIONS;
    static final List<SocketAddress> REGIONAL_ENDPOINTS;

    static {

        out.println("--------------------------------------------------------------");
        out.println("T E S T  P A R A M E T E R S");
        out.println("--------------------------------------------------------------");

        // GLOBAL_ENDPOINT

        String value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.global-endpoint",
            "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
            null);

        out.println("GLOBAL_ENDPOINT = " + value);
        assertThat(value).isNotBlank();
        GLOBAL_ENDPOINT = parseSocketAddress(value);

        // PREFERRED_REGIONS

        List<String> list = getPropertyOrEnvironmentVariableList(
            "azure.cosmos.cassandra.preferred-region-",
            "AZURE_COSMOS_CASSANDRA_PREFERRED_REGION_",
            3);

        out.println("PREFERRED_REGIONS = " + list);
        assertThat(list).isNotEmpty();
        PREFERRED_REGIONS = list;

        // LOCAL_DATACENTER

        value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.local-datacenter",
            "AZURE_COSMOS_CASSANDRA_LOCAL_DATACENTER",
            PREFERRED_REGIONS.get(0));

        out.println("LOCAL_DATACENTER = " + value);
        LOCAL_DATACENTER = value;

        // REGIONAL_ENDPOINTS

        list = getPropertyOrEnvironmentVariableList(
            "azure.cosmos.cassandra.regional-endpoint_",
            "AZURE_COSMOS_CASSANDRA_REGIONAL_ENDPOINT_",
            3);

        out.println("REGIONAL_ENDPOINTS = " + list);
        assertThat(list).isNotEmpty();
        REGIONAL_ENDPOINTS = list.stream().map(TestCommon::parseSocketAddress).collect(Collectors.toList());

        out.println();
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

        Thread.sleep(5_000L);  // allows time for schema to propagate to all regions
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
        out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        for (final Row row : rows) {
            out.printf(format,
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
     * Get the value of the specified system {@code property} or--if it is unset--environment {@code variable}.
     * <p>
     * If neither {@code property} or {@code variable} is set, {@code defaultValue} is returned.
     *
     * @param property a system property name.
     * @param variable an environment variable name.
     * @param limit    the default value--which may be {@code null}--to be used if neither {@code property} or {@code
     *                 variable} is set.
     *
     * @return The value of the specified {@code property}, the value of the specified environment {@code variable}, or
     * {@code defaultValue}.
     */
    @SuppressWarnings("SameParameterValue")
    static List<String> getPropertyOrEnvironmentVariableList(
        @NonNull final String property, @NonNull final String variable, final int limit) {

        final List<String> list = new ArrayList<>(limit);

        for (int i = 1; i <= limit; i++) {
            final String value = getPropertyOrEnvironmentVariable(property + i, variable + i, null);
            if (value == null) {
                break;
            }
            list.add(value);
        }

        return list;
    }

    /**
     * Queries data, retrying if necessary with a downgraded consistency level.
     *
     * @param session          the session for executing requests.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     *
     * @return A {@link ResultSet result set}.
     */
    @SuppressWarnings("SameParameterValue")
    @NonNull
    static ResultSet read(
        @NonNull final CqlSession session,
        final ConsistencyLevel consistencyLevel,
        @NonNull final String keyspaceName,
        @NonNull final String tableName) {

        out.printf("Reading at %s%n", consistencyLevel);

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

        out.println("Read succeeded at " + consistencyLevel);
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

        final ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
        final UUID uuid = UUID.randomUUID();

        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.putLong(uuid.getMostSignificantBits());

        return prefix + '_' + Base64.getEncoder().withoutPadding()
            .encodeToString(buffer.array())
            .replaceAll("[^_a-zA-Z0-9]", "");
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

        out.printf("Writing at %s%n", consistencyLevel);

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
        out.println("Write succeeded at " + consistencyLevel);
    }

    /**
     * Draws a line to isolate headings from rows.
     *
     * @param widths the column widths.
     */
    private static void drawLine(final int... widths) {
        for (final int width : widths) {
            for (int i = 1; i < width; i++) {
                out.print('-');
            }
            out.print('+');
        }
        out.println();
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
     * @param value a socket address of the form <i>&lt;hostname&gt;</i><b>:</b><i>&lt;port&gt;</i>
     *
     * @return a {@link Matcher Matcher} that matches the {@code hostname} and {@code port} parts of a network socket
     * address.
     */
    @NonNull
    private static Matcher matchSocketAddress(final String value) {
        final Matcher matcher = HOSTNAME_AND_PORT.matcher(value);
        assertThat(matcher.matches()).isTrue();
        return matcher;
    }

    private static InetSocketAddress parseSocketAddress(final String value) {

        final Matcher matcher = matchSocketAddress(value);

        final String hostname = matcher.group("hostname");
        final int port = Integer.parseUnsignedInt(matcher.group("port"));

        return new InetSocketAddress(hostname, port);
    }

    // endregion
}
