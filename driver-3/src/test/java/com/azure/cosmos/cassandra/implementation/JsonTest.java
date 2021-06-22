// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_HOSTNAME;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.cosmosClusterBuilder;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "JUnit makes use of all uncalled methods")
public class JsonTest {

    private static final StackTraceElement[] STACK_TRACE = Thread.currentThread().getStackTrace();

    private static final String STACK_TRACE_JSON = toJson(Arrays.stream(STACK_TRACE)
        .map(StackTraceElement::toString)
        .collect(Collectors.toList()));

    @ParameterizedTest
    @Tag("checkin")
    @MethodSource("provideClusterBuilder")
    public void canSerializeCluster(final Cluster.Builder builder) {

        try (Cluster cluster = builder.build()) {

            final Map<String, Object> observed;
            String json = toJson(cluster);

            try {
                //noinspection unchecked
                observed = Json.readValue(json, Map.class);
            } catch (final IOException error) {
                throw new AssertionError("Conversion of observed value to map failed: " + json, error);
            }

            final Map<String, Object> expected;

            json = format("{\"clusterName\":\"%s\",\"closed\":%s,\"metadata\":%s,\"metrics\":%s}",
                cluster.getClusterName(),
                cluster.isClosed(),
                toJson(cluster.getMetadata()),
                toJson(cluster.getMetrics()));

            try {
                //noinspection unchecked
                expected = Json.readValue(json, Map.class);
            } catch (final IOException error) {
                throw new AssertionError("Conversion of expected value to map failed: " + json, error);
            }

            assertThat(observed).containsOnlyKeys(expected.keySet());
            assertThat(observed.get("clusterName")).isEqualTo(expected.get("clusterName"));
            assertThat(observed.get("closed")).isEqualTo(expected.get("closed"));

            assertThat(observed.get("metadata")).isInstanceOf(Map.class);

            //noinspection unchecked
            assertThat((Map<String, Object>) observed.get("metadata"))
                .containsExactlyEntriesOf((Map<String, Object>) expected.get("metadata"));

            // Because metrics serialize without an assist from us and the difficulty of value comparisons--metrics
            // change from moment to moment--we skip all but the check that metrics are represented by a map
            // judging that more more would be overkill
            assertThat(observed.get("metrics")).isInstanceOf(Map.class);
        }
    }

    @ParameterizedTest
    @Tag("checkin")
    @MethodSource("provideClusterBuilder")
    public void canSerializeHost(final Cluster.Builder builder) {

        try (Cluster cluster = builder.build()) {

            final Optional<Host> host = cluster.getMetadata().getAllHosts().stream().findFirst();
            assertThat(host).isPresent();

            final String observed = toJson(host.get());

            final String expected = format(
                "{\"endPoint\":\"%s\",\"datacenter\":\"%s\",\"hostId\":\"%s\",\"state\":\"%s\",\"schemaVersion\":"
                    + "\"%s\",\"cassandraVersion\":\"%s\"}",
                host.get().getEndPoint(),
                host.get().getDatacenter(),
                host.get().getHostId(),
                host.get().getState(),
                host.get().getSchemaVersion(),
                host.get().getCassandraVersion());

            assertThat(observed).isEqualTo(expected);
        }
    }

    @ParameterizedTest
    @Tag("checkin")
    @MethodSource("provideClusterBuilder")
    public void canSerializeMetadata(final Cluster.Builder builder) {

        try (Cluster cluster = builder.build()) {

            final Metadata metadata = cluster.getMetadata();
            final String observed = toJson(metadata);

            final String expected = format("{\"clusterName\":\"%s\",\"hosts\":%s,\"keyspaces\":%s}",
                metadata.getClusterName(),
                toJson(metadata.getAllHosts()),
                toJson(metadata.getKeyspaces().stream().map(KeyspaceMetadata::toString).collect(Collectors.toList())));

            assertThat(observed).isEqualTo(expected);
        }
    }

    @ParameterizedTest()
    @Tag("checkin")
    @MethodSource("provideSession")
    public void canSerializeSession(@NonNull final Session input, @NonNull final String expected) {
        try {
            final String observed = toJson(input);
            assertThat(observed).isEqualTo(expected);
        } finally {
            input.getCluster().close();
        }
    }

    @ParameterizedTest()
    @Tag("checkin")
    @MethodSource("provideStatement")
    public void canSerializeStatement(@NonNull final Statement input, @NonNull final String expected) {
        final String observed = toJson(input);
        assertThat(observed).isEqualTo(expected);
    }

    @ParameterizedTest()
    @Tag("checkin")
    @MethodSource("provideThrowable")
    public void canSerializeThrowable(@NonNull final Throwable input, @NonNull final String expected) {
        final String value = toJson(input);
        assertThat(value).isEqualTo(expected);
    }

    @NonNull
    private static Throwable fixupStackTrace(final Throwable error) {
        error.setStackTrace(STACK_TRACE);
        return error;
    }

    @NonNull
    private static Stream<Arguments> provideClusterBuilder() {
        return Stream.of(Arguments.of(cosmosClusterBuilder()
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD)));
    }

    @NonNull
    private static Stream<Arguments> provideSession() {

        final Cluster cluster = cosmosClusterBuilder()
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD)
            .withoutMetrics()
            .build();

        final Session session = cluster.connect();

        return Stream.of(Arguments.of(session, format("{\"cluster\":{"
                + "\"clusterName\":%s,"
                + "\"closed\":false,"
                + "\"metadata\":%s,"
                + "\"metrics\":%s},"
                + "\"closed\":false,"
                + "\"loggedKeyspace\":null}",
            toJson(cluster.getClusterName()),
            toJson(cluster.getMetadata()),
            toJson(cluster.getMetrics()))));
    }

    @NonNull
    private static Stream<Arguments> provideStatement() {
        return Stream.of(
            Arguments.of(new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS keyspace "
                    + "WITH replication = {'class':'SimpleStrategy','replication_factor':3} "
                    + "WITH cosmosdb_provisioned_throughput=400"),
                "{\"queryString\":\"CREATE KEYSPACE IF NOT EXISTS keyspace WITH replication = "
                    + "{'class':'SimpleStrategy','replication_factor':3} WITH cosmosdb_provisioned_throughput=400\","
                    + "\"tracing\":false,\"fetchSize\":0,\"readTimeoutMillis\":-2147483648,"
                    + "\"defaultTimestamp\":-9223372036854775808}"),
            Arguments.of(QueryBuilder
                    .insertInto("keyspace", "table")
                    .values(
                        new String[] { "id", "field_1", "field_2", "field_3" },
                        new String[] { "xx", "value_1", "value_2", "value_3" })
                    .ifNotExists(),
                "{\"queryString\":\"INSERT INTO keyspace.table (id,field_1,field_2,field_3) VALUES (?,?,?,?) IF NOT "
                    + "EXISTS;\",\"tracing\":false,\"fetchSize\":0,\"readTimeoutMillis\":-2147483648,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"idempotent\":false,\"keyspace\":\"keyspace\"}"),
            Arguments.of(
                QueryBuilder
                    .select("id", "field_1", "field_2", "field_3")
                    .from("keyspace", "table"),
                "{\"queryString\":\"SELECT id,field_1,field_2,field_3 FROM keyspace.table;\",\"tracing\":false,"
                    + "\"fetchSize\":0,\"readTimeoutMillis\":-2147483648,\"defaultTimestamp\":-9223372036854775808,"
                    + "\"idempotent\":true,\"keyspace\":\"keyspace\"}"),
            Arguments.of(
                QueryBuilder
                    .update("keyspace", "table")
                    .where(QueryBuilder.eq("id", "xx"))
                    .with(QueryBuilder.set("field_1", "new-value")),
                "{\"queryString\":\"UPDATE keyspace.table SET field_1=? WHERE id=?;\",\"tracing\":false,"
                    + "\"fetchSize\":0,\"readTimeoutMillis\":-2147483648,\"defaultTimestamp\":-9223372036854775808,"
                    + "\"idempotent\":true,\"keyspace\":\"keyspace\"}"),
            Arguments.of(
                QueryBuilder
                    .delete()
                    .from("keyspace", "table")
                    .where(QueryBuilder.eq("id", "xx")),
                "{\"queryString\":\"DELETE FROM keyspace.table WHERE id=?;\",\"tracing\":false,\"fetchSize\":0,"
                    + "\"readTimeoutMillis\":-2147483648,\"defaultTimestamp\":-9223372036854775808,\"idempotent\":true,"
                    + "\"keyspace\":\"keyspace\"}"),
            Arguments.of(
                QueryBuilder.batch(
                    QueryBuilder
                        .insertInto("keyspace", "table")
                        .values(
                            new String[] { "id", "field_1", "field_2", "field_3" },
                            new String[] { "xx", "value_1", "value_2", "value_3" })
                        .ifNotExists(),
                    QueryBuilder
                        .select("id", "field_1", "field_2", "field_3")
                        .from("keyspace", "table"),
                    QueryBuilder
                        .update("keyspace", "table")
                        .where(QueryBuilder.eq("id", "xx"))
                        .with(QueryBuilder.set("field_1", "new-value")),
                    QueryBuilder
                        .delete()
                        .from("keyspace", "table")
                        .where(QueryBuilder.eq("id", "xx"))),
                "{\"queryString\":\"BEGIN BATCH INSERT INTO keyspace.table (id,field_1,field_2,field_3) VALUES (?,?,"
                    + "?,?) IF NOT EXISTS;SELECT id,field_1,field_2,field_3 FROM keyspace.table;UPDATE keyspace.table "
                    + "SET field_1=? WHERE id=?;DELETE FROM keyspace.table WHERE id=?;APPLY BATCH;\",\"tracing\":false,"
                    + "\"fetchSize\":0,\"readTimeoutMillis\":-2147483648,\"defaultTimestamp\":-9223372036854775808,"
                    + "\"idempotent\":false,\"keyspace\":\"keyspace\"}"));
    }

    @NonNull
    private static Stream<Arguments> provideThrowable() {

        final Cluster.Builder builder = cosmosClusterBuilder()
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD);

        final EndPoint globalEndPoint = builder.getContactPoints().get(0);

        return Stream.of(
            Arguments.of(
                fixupStackTrace(new AuthenticationException(
                    globalEndPoint,
                    "Indicates an error during the authentication phase while connecting to a node.")),
                format("{\"error\":\"com.datastax.driver.core.exceptions.AuthenticationException\",\"cause\":null,"
                        + "\"message\":\"Authentication error on host %s: Indicates an error during the authentication "
                        + "phase while connecting to a node.\",\"stackTrace\":%s,\"suppressed\":[],\"address\":\"%s\","
                        + "\"endPoint\":\"%s\",\"host\":\"%s\"}",
                    globalEndPoint,
                    STACK_TRACE_JSON,
                    GLOBAL_ENDPOINT,
                    globalEndPoint,
                    GLOBAL_ENDPOINT_HOSTNAME)),
            Arguments.of(
                fixupStackTrace(new InvalidQueryException(
                    globalEndPoint,
                    "Indicates a syntactically correct but invalid query.",
                    fixupStackTrace(new DriverException("Underlying cause of the InvalidQueryException")))),
                format("{\"error\":\"com.datastax.driver.core.exceptions.InvalidQueryException\",\"cause\":{"
                        + "\"error\":\"com.datastax.driver.core.exceptions.DriverException\",\"cause\":null,"
                        + "\"message\":\"Underlying cause of the InvalidQueryException\",\"stackTrace\":%s,"
                        + "\"suppressed\":[]},\"message\":\"Indicates a syntactically correct but invalid query.\","
                        + "\"stackTrace\":%s,\"suppressed\":[],\"address\":\"%s\",\"endPoint\":\"%s\",\"host\":\"%s\"}",
                    STACK_TRACE_JSON,
                    STACK_TRACE_JSON,
                    GLOBAL_ENDPOINT,
                    globalEndPoint,
                    GLOBAL_ENDPOINT_HOSTNAME)));
    }
}
