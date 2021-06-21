// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;

import java.io.File;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.String.format;
import static java.lang.System.out;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

/**
 * A utility class that implements common static methods useful for writing tests.
 */
public final class TestCommon {

    // region Fields

    public static final String GLOBAL_ENDPOINT;
    public static final InetSocketAddress GLOBAL_ENDPOINT_ADDRESS;
    public static final String GLOBAL_ENDPOINT_HOSTNAME;
    public static final int GLOBAL_ENDPOINT_PORT;
    public static final String KEYSPACE_NAME = "test_driver_4";
    public static final boolean MULTI_REGION_WRITES;
    public static final String PASSWORD;
    public static final List<String> PREFERRED_REGIONS;
    public static final List<InetSocketAddress> REGIONAL_ENDPOINTS;
    public static final String TRUSTSTORE_PASSWORD;
    public static final String TRUSTSTORE_PATH;
    public static final String USERNAME;

    private static final Pattern HOSTNAME_AND_PORT = Pattern.compile("^\\s*(?<hostname>.*?):(?<port>\\d+)\\s*$");
    private static final Map<String, String> PROPERTIES = new TreeMap<>();

    static {

        // GLOBAL_ENDPOINT

        String value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.global-endpoint",
            "localhost");

        assertThat(value).isNotBlank();
        final int index = value.lastIndexOf(':');

        if (index == -1) {
            GLOBAL_ENDPOINT = value + ":10350";
            GLOBAL_ENDPOINT_HOSTNAME = value;
            GLOBAL_ENDPOINT_PORT = 10350;
        } else {
            assertThat(index).isGreaterThan(0);
            assertThat(index).isLessThan(value.length() - 1);
            GLOBAL_ENDPOINT = value;
            GLOBAL_ENDPOINT_HOSTNAME = GLOBAL_ENDPOINT.substring(0, index);
            GLOBAL_ENDPOINT_PORT = Integer.parseUnsignedInt(GLOBAL_ENDPOINT.substring(index + 1));
        }

        GLOBAL_ENDPOINT_ADDRESS = new InetSocketAddress(GLOBAL_ENDPOINT_HOSTNAME, GLOBAL_ENDPOINT_PORT);

        setProperty("azure.cosmos.cassandra.global-endpoint-address", GLOBAL_ENDPOINT_ADDRESS);
        setProperty("azure.cosmos.cassandra.global-endpoint-hostname", GLOBAL_ENDPOINT_HOSTNAME);
        setProperty("azure.cosmos.cassandra.global-endpoint-port", GLOBAL_ENDPOINT_PORT);

        // USERNAME

        value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.username",
            null);

        assertThat(value).isNotBlank();
        USERNAME = value;

        // PASSWORD

        value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.password",
            null);

        assertThat(value).isNotBlank();
        PASSWORD = value;

        // MULTI_REGION_WRITES

        value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.multi-region-writes",
            "true");

        assertThat(value).isNotBlank();
        MULTI_REGION_WRITES = Boolean.parseBoolean(value);

        // PREFERRED_REGIONS

        List<String> list = getPropertyOrEnvironmentVariableList(
            "azure.cosmos.cassandra.preferred-regions",
            "azure.cosmos.cassandra.preferred-region-");

        assertThat(list).isNotEmpty();
        PREFERRED_REGIONS = list;

        // REGIONAL_ENDPOINTS

        list = getPropertyOrEnvironmentVariableList(
            "azure.cosmos.cassandra.regional-endpoints",
            "azure.cosmos.cassandra.regional-endpoint-");

        assertThat(list).isNotEmpty();
        REGIONAL_ENDPOINTS = list.stream().map(TestCommon::parseInetSocketAddress).collect(Collectors.toList());

        value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.truststore-path",
            null);

        assertThat(value).isNotEmpty();
        assertThat(new File(value)).exists().canRead();
        TRUSTSTORE_PATH = value;

        value = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.truststore-password",
            null);

        assertThat(value).isNotEmpty();
        TRUSTSTORE_PASSWORD = value;
    }

    private TestCommon() {
        throw new UnsupportedOperationException();
    }

    // endregion

    // region Methods

    /**
     * Logs the name of each test before it is executed.
     *
     * @param info Test info.
     * @param log  The logger for the test.
     */
    public static void logTestName(final TestInfo info, final Logger log) {
        log.info("---------------------------------------------------------------------------------------------------");
        log.info("{}", info.getTestMethod().orElseGet(() -> fail("expected test to be called with test method")));
        log.info("---------------------------------------------------------------------------------------------------");
    }

    /**
     * Prints the set of test parameters to {@link System#out} before all tests are run.
     * <p>
     * This method should be called in a test method marked with {@link BeforeAll}. It is not automatically called here.
     */
    public static void printTestParameters() {
        out.println("--------------------------------------------------------------");
        out.println("T E S T  P A R A M E T E R S");
        out.println("--------------------------------------------------------------");

        for (final Map.Entry<String, String> property : PROPERTIES.entrySet()) {
            out.println(property.getKey() + " = " + toJson(property.getValue()));
        }

        out.println();
    }

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     *
     * @param session      the session for executing requests.
     * @param keyspaceName name of the keyspace to query.
     * @param tableName    name of the table to query.
     */
    static void createSchema(
        final CqlSession session,
        final String keyspaceName,
        final String tableName,
        final int throughput) {

        try {

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
                    + ") with cosmosdb_provisioned_throughput=%s",
                keyspaceName,
                tableName,
                throughput));

            Thread.sleep(5_000L);  // allows time for schema propagation

        } catch (final Throwable error) {
            fail("could not create schema due to: {}", toJson(error));
        }
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
     * @param defaultValue the default value--which may be {@code null}--to be used if neither {@code property} or
     *                     {@code variable} is set.
     *
     * @return The value of the specified {@code property}, the value of the specified environment {@code variable}, or
     * {@code defaultValue}.
     */
    static String getPropertyOrEnvironmentVariable(@NonNull final String property, final String defaultValue) {

        final String variable = getVariableName(property);
        String value = System.getProperty(property);

        if (value == null || value.isEmpty()) {
            value = System.getenv(variable);
        }

        if (value == null) {
            value = defaultValue;
        }

        if (value != null) {
            PROPERTIES.put(property, value);
            System.setProperty(property, value);
            System.setProperty(variable, value);
        } else {
            PROPERTIES.remove(property);
            System.getProperties().remove(property);
            System.getProperties().remove(variable);
        }

        return value;
    }

    /**
     * Get the value of the specified system {@code property} or--if it is unset--environment {@code variable}.
     * <p>
     * If neither {@code property} or {@code variable} is set, {@code defaultValue} is returned.
     *
     * @param property a system property name.
     * @param elementPrefix a prefix for elements in the list of values.
     *
     * @return The value of the specified {@code property}, the value of the specified environment {@code variable}, or
     * {@code defaultValue}.
     */
    @SuppressWarnings("SameParameterValue")
    static List<String> getPropertyOrEnvironmentVariableList(
        @NonNull final String property,
        @NonNull final String elementPrefix) {

        final String[] array = getPropertyOrEnvironmentVariable(property, "").split(("\\s*,\\s*"));
        int elementNumber = 1;

        for (final String elementValue : array) {

            final String elementName = elementPrefix + elementNumber;

            PROPERTIES.put(elementName, elementValue);
            System.setProperty(elementName, elementValue);
            System.setProperty(getVariableName(elementName), elementValue);

            elementNumber++;
        }

        return Arrays.asList(array);
    }

    /**
     * Queries data, retrying if necessary with a downgraded consistency level.
     *
     * @param session          the session for executing requests.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     *
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @return A {@link ResultSet result set}.
     */
    @SuppressWarnings("SameParameterValue")
    @NonNull
    static ResultSet read(
        @NonNull final CqlSession session,
        @NonNull final String keyspaceName, @NonNull final String tableName, final ConsistencyLevel consistencyLevel) {

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
     * Queries data, retrying if necessary with a downgraded consistency level asynchronously.
     *
     * @param session          the session for executing requests.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     *
     * @return {@link CompletableFuture Promise} of an {@link AsyncResultSet asynchronous result set}.
     */
    @SuppressWarnings("SameParameterValue")
    @NonNull
    static CompletableFuture<AsyncResultSet> readAsync(
        @NonNull final CqlSession session,
        final ConsistencyLevel consistencyLevel,
        @NonNull final String keyspaceName,
        @NonNull final String tableName) {

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

        return session.executeAsync(statement).toCompletableFuture();
    }

    @SuppressWarnings("SameParameterValue")
    static void testAllStatements(@NonNull final CqlSession session) {

        final String tableName = uniqueName("sensor_data_");

        try {

            createSchema(session, KEYSPACE_NAME, tableName, 10_000);

            // SimpleStatements

            session.execute(SimpleStatement.newInstance(format(
                "SELECT * FROM %s.%s WHERE sensor_id=uuid() and date=toDate(now())",
                KEYSPACE_NAME,
                tableName)));

            session.execute(SimpleStatement.newInstance(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
                KEYSPACE_NAME,
                tableName)));

            session.execute(SimpleStatement.newInstance(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp=toTimestamp("
                    + "now())",
                KEYSPACE_NAME,
                tableName)));

            session.execute(SimpleStatement.newInstance(format(
                "DELETE FROM %s.%s WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp=toTimestamp(now())",
                KEYSPACE_NAME,
                tableName)));

            // Built statements

            final LocalDate date = LocalDate.of(2016, 6, 30);
            final Instant timestamp = Instant.now();
            final UUID uuid = UUID.randomUUID();

            final Select select = QueryBuilder.selectFrom(KEYSPACE_NAME, tableName)
                .all()
                .whereColumn("sensor_id").isEqualTo(literal(uuid))
                .whereColumn("date").isEqualTo(literal(date))
                .whereColumn("timestamp").isEqualTo(literal(timestamp));

            session.execute(select.build());

            final Insert insert = QueryBuilder.insertInto(KEYSPACE_NAME, tableName)
                .value("sensor_id", literal(uuid))
                .value("date", literal(date))
                .value("timestamp", literal(timestamp));

            session.execute(insert.build());

            final Update update = QueryBuilder.update(KEYSPACE_NAME, tableName)
                .setColumn("value", literal(1.0))
                .whereColumn("sensor_id").isEqualTo(literal(uuid))
                .whereColumn("date").isEqualTo(literal(date))
                .whereColumn("timestamp").isEqualTo(literal(timestamp));

            session.execute(update.build());

            final Delete delete = QueryBuilder.deleteFrom(KEYSPACE_NAME, tableName)
                .whereColumn("sensor_id").isEqualTo(literal(uuid))
                .whereColumn("date").isEqualTo(literal(date))
                .whereColumn("timestamp").isEqualTo(literal(timestamp));

            session.execute(delete.build());

            // BoundStatements

            PreparedStatement preparedStatement = session.prepare(format(
                "SELECT * FROM %s.%s WHERE sensor_id = ? and date = ?",
                KEYSPACE_NAME,
                tableName));

            BoundStatement boundStatement = preparedStatement.bind(uuid, date);
            session.execute(boundStatement);

            preparedStatement = session.prepare(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (?, ?, ?)",
                KEYSPACE_NAME,
                tableName));

            boundStatement = preparedStatement.bind(uuid, date, timestamp);
            session.execute(boundStatement);

            preparedStatement = session.prepare(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = ? AND date = ? AND timestamp = ?",
                KEYSPACE_NAME,
                tableName));

            boundStatement = preparedStatement.bind(uuid, date, timestamp);
            session.execute(boundStatement);

            preparedStatement = session.prepare(format(
                "DELETE FROM %s.%s WHERE sensor_id = ? AND date = ? AND timestamp = ?",
                KEYSPACE_NAME,
                tableName));

            boundStatement = preparedStatement.bind(uuid, date, timestamp);
            session.execute(boundStatement);

            // BatchStatement (NOTE: BATCH requests must be single table Update/Delete/Insert statements)

            final BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED)
                .add(boundStatement)
                .add(boundStatement);

            session.execute(batchStatement);

        } finally {
            assertThatCode(() -> session.execute(SchemaBuilder.dropTable(KEYSPACE_NAME, tableName)
                .ifExists()
                .build()))
                .doesNotThrowAnyException();
        }
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

        final UUID uuid = UUID.randomUUID();
        final long id = uuid.getLeastSignificantBits() ^ uuid.getMostSignificantBits();

        return prefix + Long.toUnsignedString(id, Character.MAX_RADIX);
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *  @param session          the session for executing requests.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     */
    @SuppressWarnings("SameParameterValue")
    static void write(
        @NonNull final CqlSession session,
        @NonNull final String keyspaceName, @NonNull final String tableName, final ConsistencyLevel consistencyLevel) {

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
                out.print('-');
            }
            out.print('+');
        }
        out.println();
    }

    private static String getVariableName(final String property) {

        final StringBuilder builder = new StringBuilder(property.length());

        property.chars().forEachOrdered(c -> {
            builder.appendCodePoint(c == '.' || c == '-' ? '_' : Character.toUpperCase(c));
        });

        return builder.toString();
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

    private static InetSocketAddress parseInetSocketAddress(final String value) {

        final Matcher matcher = matchSocketAddress(value);

        final String hostname = matcher.group("hostname");
        final int port = Integer.parseUnsignedInt(matcher.group("port"));

        return new InetSocketAddress(hostname, port);
    }

    private static void setProperty(@NonNull final String name, @NonNull final Object value) {
        final String string = value.toString();
        PROPERTIES.put(name, string);
        System.setProperty(name, string);
        System.setProperty(getVariableName(name), string);
    }

    // endregion
}
