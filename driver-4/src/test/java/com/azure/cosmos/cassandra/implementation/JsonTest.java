// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "JUnit makes use of all uncalled methods")
public class JsonTest {

    private static final StackTraceElement[] STACK_TRACE = Thread.currentThread().getStackTrace();

    private static final String STACK_TRACE_JSON = toJson(Arrays.stream(STACK_TRACE)
        .map(StackTraceElement::toString)
        .collect(Collectors.toList()));

    @ParameterizedTest()
    @Tag("checkin")
    @MethodSource("provideStatements")
    public void canSerializeRequest(@NonNull final Request input, @NonNull final String expected) {
        final String observed = toJson(input);
        assertThat(observed).isEqualTo(expected);
    }

    @SuppressWarnings({ "TryFinallyCanBeTryWithResources" })
    @ParameterizedTest
    @Tag("checkin")
    @MethodSource("provideSession")
    public void canSerializeSession(@NonNull final Session input, @NonNull final String expected) {
        try {
            final String observed = toJson(input);
            assertThat(observed).isEqualTo(expected);
        } finally {
            input.close();
        }
    }

    @ParameterizedTest()
    @Tag("checkin")
    @MethodSource("provideThrowables")
    public void canSerializeThrowable(@NonNull final Throwable input, @NonNull final String expected) {
        final String observed = toJson(input);
        assertThat(observed).isEqualTo(expected);
    }

    @NonNull
    private static Throwable fixupStackTrace(final Throwable error) {
        error.setStackTrace(STACK_TRACE);
        return error;
    }

    @NonNull
    private static Stream<Arguments> provideSession() {

        final CqlSession session = CqlSession.builder().build();

        assertThatCode(() ->
            // Give the driver time to initialize all regional endpoints
            // node addresses are not resolved until then
            Thread.sleep(2_000)).doesNotThrowAnyException();

        final DefaultDriverContext driverContext = (DefaultDriverContext) session.getContext();
        final Metadata metadata = session.getMetadata();

        return Stream.of(Arguments.of(session, format("{\"name\":%s,"
                + "\"context\":{"
                + "\"sessionName\":%s,"
                + "\"protocolVersion\":%s,"
                + "\"startupOptions\":%s,"
                + "\"authProvider\":%s,"
                + "\"loadBalancingPolicies\":{\"default\":%s},"
                + "\"reconnectionPolicy\":%s,"
                + "\"requestThrottler\":%s,"
                + "\"requestTracker\":%s,"
                + "\"retryPolicies\":{\"default\":%s},"
                + "\"speculativeExecutionPolicies\":{\"default\":%s}"
                + "},"
                + "\"keyspace\":null,"
                + "\"metadata\":{"
                + "\"nodes\":%s,"
                + "\"keyspaces\":%s,"
                + "\"tokenMap\":null,"
                + "\"clusterName\":%s"
                + "},"
                + "\"metrics\":%s"
                + "}",
            toJson(session.getName()),
            toJson(driverContext.getSessionName()),
            toJson(driverContext.getProtocolVersion()),
            toJson(driverContext.getStartupOptions()),
            toJson(driverContext.getAuthProvider()),
            toJson(driverContext.getLoadBalancingPolicy("default")),
            toJson(driverContext.getReconnectionPolicy()),
            toJson(driverContext.getRequestThrottler()),
            toJson(driverContext.getRequestTracker()),
            toJson(driverContext.getRetryPolicy("default")),
            toJson(driverContext.getSpeculativeExecutionPolicy("default")),
            toJson(metadata.getNodes()),
            toJson(metadata.getKeyspaces()),
            toJson(metadata.getClusterName()),
            toJson(session.getMetrics()))));
    }

    @NonNull
    private static Stream<Arguments> provideStatements() {
        return Stream.of(
            Arguments.of(
                SimpleStatement.newInstance("CREATE KEYSPACE IF NOT EXISTS test_driver_4 "
                    + "WITH replication = {'class':'SimpleStrategy','replication_factor':3} "
                    + "WITH cosmosdb_provisioned_throughput=400"),
                "{\"query\":\"CREATE KEYSPACE IF NOT EXISTS test_driver_4 WITH replication = "
                    + "{'class':'SimpleStrategy','replication_factor':3} WITH cosmosdb_provisioned_throughput=400\","
                    + "\"namedValues\":{},\"positionalValues\":[],\"consistencyLevel\":null,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":null,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false}"),
            Arguments.of(
                QueryBuilder.insertInto("test_driver_4", "table_name")
                    .value("id", literal("xx"))
                    .value("field_1", literal("value_1"))
                    .value("field_2", literal("value_2"))
                    .value("field_3", literal("value_3"))
                    .ifNotExists()
                    .build(),
                "{\"query\":\"INSERT INTO test_driver_4.table_name (id,field_1,field_2,field_3) VALUES ('xx',"
                    + "'value_1','value_2','value_3') IF NOT EXISTS\",\"namedValues\":{},\"positionalValues\":[],"
                    + "\"consistencyLevel\":null,\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":false,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false}"),
            Arguments.of(
                QueryBuilder.selectFrom("test_driver_4", "table_name")
                    .columns("id", "field_1", "field_2", "field_3")
                    .build(),
                "{\"query\":\"SELECT id,field_1,field_2,field_3 FROM test_driver_4.table_name\",\"namedValues\":{},"
                    + "\"positionalValues\":[],\"consistencyLevel\":null,\"defaultTimestamp\":-9223372036854775808,"
                    + "\"executionProfile\":null,\"executionProfileName\":null,\"fetchSize\":-2147483648,"
                    + "\"idempotent\":true,\"keyspace\":null,\"node\":null,\"nowInSeconds\":-2147483648,"
                    + "\"pageSize\":-2147483648,\"pagingState\":null,\"queryTimestamp\":-9223372036854775808,"
                    + "\"routingKeyspace\":null,\"routingToken\":null,\"serialConsistencyLevel\":null,\"timeout\":null,"
                    + "\"tracing\":false}"),
            Arguments.of(
                QueryBuilder.update("test_driver_4", "table_name")
                    .setColumn("field_1", literal("new-value"))
                    .whereColumn("id").isEqualTo(literal("xx"))
                    .build(),
                "{\"query\":\"UPDATE test_driver_4.table_name SET field_1='new-value' WHERE id='xx'\","
                    + "\"namedValues\":{},\"positionalValues\":[],\"consistencyLevel\":null,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":true,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false}"),
            Arguments.of(
                QueryBuilder.deleteFrom("test_driver_4", "table_name")
                    .whereColumn("id").isEqualTo(literal("xx"))
                    .build(),
                "{\"query\":\"DELETE FROM test_driver_4.table_name WHERE id='xx'\",\"namedValues\":{},"
                    + "\"positionalValues\":[],\"consistencyLevel\":null,\"defaultTimestamp\":-9223372036854775808,"
                    + "\"executionProfile\":null,\"executionProfileName\":null,\"fetchSize\":-2147483648,"
                    + "\"idempotent\":true,\"keyspace\":null,\"node\":null,\"nowInSeconds\":-2147483648,"
                    + "\"pageSize\":-2147483648,\"pagingState\":null,\"queryTimestamp\":-9223372036854775808,"
                    + "\"routingKeyspace\":null,\"routingToken\":null,\"serialConsistencyLevel\":null,\"timeout\":null,"
                    + "\"tracing\":false}"),
            Arguments.of(
                BatchStatement.newInstance(BatchType.UNLOGGED)
                    .addAll(
                        QueryBuilder.insertInto("test_driver_4", "table_name")
                            .value("id", literal("xx"))
                            .value("field_1", literal("value_1"))
                            .value("field_2", literal("value_2"))
                            .value("field_3", literal("value_3"))
                            .ifNotExists()
                            .build(),
                        QueryBuilder.insertInto("test_driver_4", "table_name")
                            .value("id", literal("xx"))
                            .value("field_1", literal("value_1"))
                            .value("field_2", literal("value_2"))
                            .value("field_3", literal("value_3"))
                            .ifNotExists()
                            .build(),
                        QueryBuilder.selectFrom("test_driver_4", "table_name")
                            .columns("id", "field_1", "field_2", "field_3")
                            .build(),
                        QueryBuilder.update("test_driver_4", "table_name")
                            .setColumn("field_1", literal("new-value"))
                            .whereColumn("id").isEqualTo(literal("xx"))
                            .build()),
                "{\"batchType\":\"UNLOGGED\",\"statements\":[{\"query\":\"INSERT INTO test_driver_4.table_name (id,"
                    + "field_1,field_2,field_3) VALUES ('xx','value_1','value_2','value_3') IF NOT EXISTS\","
                    + "\"namedValues\":{},\"positionalValues\":[],\"consistencyLevel\":null,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":false,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false},{\"query\":\"INSERT INTO "
                    + "test_driver_4.table_name (id,field_1,field_2,field_3) VALUES ('xx','value_1','value_2',"
                    + "'value_3') IF NOT EXISTS\",\"namedValues\":{},\"positionalValues\":[],\"consistencyLevel\":null,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":false,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false},{\"query\":\"SELECT id,"
                    + "field_1,field_2,field_3 FROM test_driver_4.table_name\",\"namedValues\":{},"
                    + "\"positionalValues\":[],\"consistencyLevel\":null,\"defaultTimestamp\":-9223372036854775808,"
                    + "\"executionProfile\":null,\"executionProfileName\":null,\"fetchSize\":-2147483648,"
                    + "\"idempotent\":true,\"keyspace\":null,\"node\":null,\"nowInSeconds\":-2147483648,"
                    + "\"pageSize\":-2147483648,\"pagingState\":null,\"queryTimestamp\":-9223372036854775808,"
                    + "\"routingKeyspace\":null,\"routingToken\":null,\"serialConsistencyLevel\":null,\"timeout\":null,"
                    + "\"tracing\":false},{\"query\":\"UPDATE test_driver_4.table_name SET field_1='new-value' WHERE "
                    + "id='xx'\",\"namedValues\":{},\"positionalValues\":[],\"consistencyLevel\":null,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":true,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false}],\"consistencyLevel\":null,"
                    + "\"defaultTimestamp\":-9223372036854775808,\"executionProfile\":null,"
                    + "\"executionProfileName\":null,\"fetchSize\":-2147483648,\"idempotent\":null,\"keyspace\":null,"
                    + "\"node\":null,\"nowInSeconds\":-2147483648,\"pageSize\":-2147483648,\"pagingState\":null,"
                    + "\"queryTimestamp\":-9223372036854775808,\"routingKeyspace\":null,\"routingToken\":null,"
                    + "\"serialConsistencyLevel\":null,\"timeout\":null,\"tracing\":false}"));
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", justification = "False alarm on Java 11")
    @NonNull
    private static Stream<Arguments> provideThrowables() {

        try (CqlSession session = CqlSession.builder().build()) {

            assertThatCode(() ->
                // Give the driver time to initialize all regional endpoints
                // node addresses are not resolved until then
                Thread.sleep(2_000)).doesNotThrowAnyException();

            return Stream.of(
                Arguments.of(
                    fixupStackTrace(AllNodesFailedException.fromErrors(
                        session.getMetadata().getNodes().values().stream()
                            .map(node -> new SimpleImmutableEntry<Node, Throwable>(node, new ClosedConnectionException(
                                "Indicates the connection on which a request was executing is closed")))
                            .collect(Collectors.toList()))),
                    format("{\"error\":\"com.datastax.oss.driver.api.core.AllNodesFailedException\",\"cause\":null,"
                            + "\"message\":\"All %d node(s) tried for the query failed (showing first 3 nodes, use "
                            + "getAllErrors() for more): %s\",\"stackTrace\":%s,\"suppressed\":[{\"error\":\"com"
                            + ".datastax.oss.driver.api.core.connection.ClosedConnectionException\",\"cause\":null,"
                            + "\"message\":\"Indicates the connection on which a request was executing is closed\","
                            + "\"stackTrace\":[],\"suppressed\":[],\"executionInfo\":null},{\"error\":\"com.datastax"
                            + ".oss.driver.api.core.connection.ClosedConnectionException\",\"cause\":null,"
                            + "\"message\":\"Indicates the connection on which a request was executing is closed\","
                            + "\"stackTrace\":[],\"suppressed\":[],\"executionInfo\":null},{\"error\":\"com.datastax"
                            + ".oss.driver.api.core.connection.ClosedConnectionException\",\"cause\":null,"
                            + "\"message\":\"Indicates the connection on which a request was executing is closed\","
                            + "\"stackTrace\":[],\"suppressed\":[],\"executionInfo\":null}],\"allErrors\":{%s},"
                            + "\"errors\":{%s},\"executionInfo\":null}",
                        session.getMetadata().getNodes().size(),
                        session.getMetadata().getNodes().values().stream()
                            .map(node -> node.toString() + ": ["
                                + "com.datastax.oss.driver.api.core.connection.ClosedConnectionException: "
                                + "Indicates the connection on which a request was executing is closed"
                                + "]")
                            .collect(Collectors.joining(", ")),
                        STACK_TRACE_JSON,
                        session.getMetadata().getNodes().values().stream()
                            .map(node -> toJson(node.toString()) + ":[{\"error\":\"com.datastax.oss.driver.api.core."
                                + "connection.ClosedConnectionException\",\"cause\":null,\"message\":\"Indicates the "
                                + "connection on which a request was executing is closed\",\"stackTrace\":[],"
                                + "\"suppressed\":[],\"executionInfo\":null}]")
                            .collect(Collectors.joining(",")),
                        session.getMetadata().getNodes().values().stream()
                            .limit(3)
                            .map(node -> toJson(node.toString()) + ":{\"error\":\"com.datastax.oss.driver.api.core."
                                + "connection.ClosedConnectionException\",\"cause\":null,\"message\":\"Indicates the "
                                + "connection on which a request was executing is closed\",\"stackTrace\":[],"
                                + "\"suppressed\":[],\"executionInfo\":null}")
                            .collect(Collectors.joining(",")))));
        }
    }
}
