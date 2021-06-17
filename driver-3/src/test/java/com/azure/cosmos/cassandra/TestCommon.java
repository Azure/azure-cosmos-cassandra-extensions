// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static java.lang.String.format;
import static java.lang.System.out;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

/**
 * A utility class that implements common static methods useful for writing tests.
 */
public final class TestCommon {

    private TestCommon() {
        throw new UnsupportedOperationException();
    }

    // region Fields

    public static final String GLOBAL_ENDPOINT;
    public static final String GLOBAL_ENDPOINT_HOSTNAME;
    public static final int GLOBAL_ENDPOINT_PORT;
    public static final String USERNAME;
    public static final String PASSWORD;
    public static final boolean MULTI_REGION_WRITES;
    public static final List<String> PREFERRED_REGIONS;
    public static final List<SocketAddress> REGIONAL_ENDPOINTS;
    public static final String TRUSTSTORE_PATH;
    public static final String TRUSTSTORE_PASSWORD;

    public static final String KEYSPACE_NAME = "test_driver_3";

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
        REGIONAL_ENDPOINTS = list.stream().map(TestCommon::parseSocketAddress).collect(Collectors.toList());

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

        out.println("--------------------------------------------------------------");
        out.println("T E S T  P A R A M E T E R S");
        out.println("--------------------------------------------------------------");

        for (final Map.Entry<String, String> property : PROPERTIES.entrySet()) {
            out.println(property.getKey() + " = " + toJson(property.getValue()));
        }

        out.println();
    }

    private static void setProperty(@NonNull final String name, @NonNull final Object value) {
        final String string = value.toString();
        PROPERTIES.put(name, string);
        System.setProperty(name, string);
        System.setProperty(getVariableName(name), string);
    }

    // endregion

    // region Methods

    public static Cluster.Builder cosmosClusterBuilder() {
        return cosmosClusterBuilder(CosmosLoadBalancingPolicy.defaultPolicy(), CosmosRetryPolicy.defaultPolicy());
    }

    public static Cluster.Builder cosmosClusterBuilder(final CosmosLoadBalancingPolicy loadBalancingPolicy) {
        return cosmosClusterBuilder(loadBalancingPolicy, CosmosRetryPolicy.defaultPolicy());
    }

    public static Cluster.Builder cosmosClusterBuilder(
        @NonNull final CosmosLoadBalancingPolicy loadBalancingPolicy,
        @NonNull final CosmosRetryPolicy retryPolicy) {

        return Cluster.builder()
            .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(30_000).setReadTimeoutMillis(30_000))
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withPoolingOptions(new PoolingOptions()
                .setConnectionsPerHost(LOCAL, 10, 10)
                .setConnectionsPerHost(REMOTE, 1, 10))
            .withReconnectionPolicy(new ConstantReconnectionPolicy(1_000))
            .withRetryPolicy(retryPolicy)
            .withPort(GLOBAL_ENDPOINT_PORT)
            .withSSL();
    }

    /**
     * Closes the given {@link Session} after dropping the given {@code tableName}.
     *
     * @param session   Session to be closed.
     * @param tableName Name of table to be dropped before closing {@code session}.
     */
    static void cleanUp(@NonNull final Session session, @NonNull final String tableName) {

        assertThat(session).isNotNull();
        assertThat(tableName).isNotBlank();
        assertThat(session.isClosed()).isFalse();

        try {
            session.execute(SchemaBuilder.dropTable(KEYSPACE_NAME, tableName).ifExists());
        } catch (final Throwable error) {
            fail("Failed to drop table " + tableName + " due to: " + error);
        } finally {
            session.close();
        }
    }

    /**
     * Creates the keyspace and table used to verify that we can integrate with a Cosmos Cassandra API instance.
     */
    static void createSchema(
        final Session session, final String tableName) throws InterruptedException {

        session.execute(format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':3}",
            KEYSPACE_NAME));

        Thread.sleep(5_000);

        session.execute(format(
            "CREATE TABLE IF NOT EXISTS %s.%s ("
                + "sensor_id uuid,"
                + "date date,"
                + "timestamp timestamp,"  // emulates bucketing by day
                + "value double,"
                + "PRIMARY KEY ((sensor_id,date),timestamp)"
                + ")",
            KEYSPACE_NAME,
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
        out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        for (final Row row : rows) {
            out.printf(format,
                row.getUUID("sensor_id"),
                row.getDate("date"),
                sdf.format(row.getTimestamp("timestamp")),
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

    private static String getVariableName(final String property) {

        final StringBuilder builder = new StringBuilder(property.length());

        property.chars().forEachOrdered(c -> {
            builder.appendCodePoint(c == '.' || c == '-' ? '_' : Character.toUpperCase(c));
        });

        return builder.toString();
    }

    /**
     * Queries data, retrying if necessary with a downgraded consistency level.
     *
     * @param session          the session for executing requests.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
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

    /**
     * Returns a unique name composed of a {@code prefix} string and a {@linkplain UUID#randomUUID random UUID}.
     * <p>
     * Hyphens are removed from the generated {@link UUID} before it is joined to the {@code prefix} with an underscore.
     *
     * @param prefix a string that starts the unique name.
     *
     * @return a unique name of the form <i>&lt;prefix&gt;</i><b><code>_</code></b><i>&lt;random-uuid&gt;</i>.
     */
    static String uniqueName(final String prefix) {
        final UUID uuid = UUID.randomUUID();
        final long suffix = uuid.getLeastSignificantBits() ^ uuid.getMostSignificantBits();
        return prefix + '_' + Long.toUnsignedString(suffix, Character.MAX_RADIX);
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param session          the session for executing requests.
     * @param consistencyLevel the consistency level to apply or {@code null}.
     * @param keyspaceName     name of the keyspace to query.
     * @param tableName        name of the table to query.
     */
    static void write(
        final Session session,
        final ConsistencyLevel consistencyLevel,
        final String keyspaceName,
        final String tableName) {

        out.printf("Writing at %s%n", consistencyLevel);

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

    static void testAllStatements(@NonNull final Session session) {

        assertThat(session).isNotNull();
        final String tableName = uniqueName("sensor_data_");

        try {
            assertThatCode(() -> createSchema(session, tableName)).doesNotThrowAnyException();

            // SimpleStatements

            SimpleStatement simpleStatement = new SimpleStatement(format(
                "SELECT * FROM %s.%s WHERE sensor_id=uuid() and date=toDate(now())",
                KEYSPACE_NAME,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
                KEYSPACE_NAME,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp="
                    + "toTimestamp(now())",
                KEYSPACE_NAME,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "DELETE FROM %s.%s WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp=toTimestamp(now())",
                KEYSPACE_NAME,
                tableName));
            session.execute(simpleStatement);

            // BuiltStatements

            final UUID uuid = UUID.randomUUID();
            final LocalDate date = LocalDate.fromYearMonthDay(2016, 06, 30);
            final Date timestamp = new Date();

            final Clause pk1Clause = QueryBuilder.eq("sensor_id", uuid);
            final Clause pk2Clause = QueryBuilder.eq("date", date);
            final Clause ckClause = QueryBuilder.eq("timestamp", 1000);

            final Select select = QueryBuilder.select()
                .all()
                .from(KEYSPACE_NAME, tableName);

            select.where(pk1Clause).and(pk2Clause).and(ckClause);
            session.execute(select);

            final Insert insert = QueryBuilder.insertInto(KEYSPACE_NAME, tableName);
            insert.values(new String[] { "sensor_id", "date", "timestamp" }, new Object[] { uuid, date, 1000 });
            session.execute(insert);

            final Update update = QueryBuilder.update(KEYSPACE_NAME, tableName);
            update.with(set("value", 1.0)).where(pk1Clause).and(pk2Clause).and(ckClause);
            session.execute(update);

            final Delete delete = QueryBuilder.delete().from(KEYSPACE_NAME, tableName);
            delete.where(pk1Clause).and(pk2Clause).and(ckClause);
            session.execute(delete);

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

            // BatchStatement

            final BatchStatement batchStatement = new BatchStatement(UNLOGGED)
                .add(simpleStatement)
                .add(boundStatement);
            session.execute(batchStatement);

        } finally {
            cleanUp(session, KEYSPACE_NAME);
        }
    }

    // endregion
}
