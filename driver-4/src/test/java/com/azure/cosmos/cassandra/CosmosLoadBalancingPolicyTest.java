// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.implementation.Json;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.REGIONAL_ENDPOINTS;
import static com.azure.cosmos.cassandra.TestCommon.uniqueName;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test illustrates use of the {@link CosmosLoadBalancingPolicy} class.
 * <p>
 * Ensure that your test runner does not set the module path when running this test. If you see a -p option on the
 * {@code java} command line, remote it. The test classes in this suite will not load the {@code referenc.conf} that
 * is included with azure-cosmos-cassandra-driver-4-extensions.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li> A Cosmos DB Cassandra API account is required. It should have at least two regions and may be configured for
 * multi-region writes or not. A number of system or--alternatively--environment variables must be set. See {@code
 * src/test/resources/application.conf} and {@link TestCommon} for a complete list. Their use and meaning should be
 * apparent from the relevant sections of the configuration and code.
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public final class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);
    private static final int TIMEOUT_IN_SECONDS = 45;

    // endregion

    // region Methods

    @BeforeEach
    public void logTestName(final TestInfo info) {
        LOG.info("---------------------------------------------------------------------------------------------------");
        LOG.info("{}", info.getTestMethod().orElseGet(() -> fail("expected test to be called with test method")));
        LOG.info("---------------------------------------------------------------------------------------------------");
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} specifying a {@code global-endpoint} (with no {@code
     * read-datacenter} or {@code write-datacenter}) routes requests correctly.
     *
     * @param multiRegionWrites {@code true}, if the test should be run with multi-region writes enabled; otherwise
     *                          {@code false}.
     *
     * @throws InterruptedException if the test is interrupted.
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testPreferredRegions(final boolean multiRegionWrites) throws InterruptedException {

        // TODO (DANOBLE) Add check that routing occurs as expected.

        final DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(
                CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES,
                multiRegionWrites)
            .build();

        try (final CqlSession session = this.connect(configLoader, multiRegionWrites)) {
            this.testAllStatements(session, uniqueName("preferred_regions"));
        }
    }

    // endregion

    // region Privates

    private static DriverConfigLoader checkState(final DriverConfigLoader configLoader) {

        final DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();

        assertThat(profile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS))
            .isEqualTo(CosmosLoadBalancingPolicy.class.getName());

        assertThat(profile.getStringList(CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS))
            .isEqualTo(PREFERRED_REGIONS);

        return configLoader;
    }

    private static CqlSession checkState(final CqlSession session, final boolean multiRegionWrites) {

        final DriverContext driverContext = session.getContext();
        final Map<UUID, Node> nodes = session.getMetadata().getNodes();
        final String profileName = driverContext.getConfig().getDefaultProfile().getName();

        // Check that the driver has got the nodes we expect: one per region because we're connected to Cosmos DB

        assertThat(nodes.values().stream().map(node -> node.getEndPoint().resolve())).containsAll(REGIONAL_ENDPOINTS);
        assertThat(REGIONAL_ENDPOINTS.size()).isEqualTo(PREFERRED_REGIONS.size()); // sanity check on test parameters
        assertThat(nodes.size()).isEqualTo(REGIONAL_ENDPOINTS.size());

        // Check that we've got the load balancing policy we think we have

        final LoadBalancingPolicy loadBalancingPolicy = driverContext.getLoadBalancingPolicy(profileName);

        assertThat(loadBalancingPolicy).isExactlyInstanceOf(CosmosLoadBalancingPolicy.class);

        final CosmosLoadBalancingPolicy cosmosLoadBalancingPolicy = (CosmosLoadBalancingPolicy) loadBalancingPolicy;
        final List<Node> nodesForReading = cosmosLoadBalancingPolicy.getNodesForReading();

        assertThat(cosmosLoadBalancingPolicy.getMultiRegionWrites()).isEqualTo(multiRegionWrites);
        assertThat(cosmosLoadBalancingPolicy.getPreferredRegions()).isEqualTo(PREFERRED_REGIONS);

        assertThat(nodesForReading.size()).isEqualTo(PREFERRED_REGIONS.size());
        assertThat(nodesForReading.stream().map(Node::getDatacenter)).containsSequence(PREFERRED_REGIONS);

        if (multiRegionWrites) {
            assertThat(cosmosLoadBalancingPolicy.getNodesForWriting()).isEqualTo(nodesForReading);
        } else {
            final List<Node> nodesForWriting = cosmosLoadBalancingPolicy.getNodesForWriting();
            assertThat(nodesForWriting.size()).isEqualTo(1);
            final SocketAddress address = nodesForWriting.get(0).getEndPoint().resolve();
            assertThat(address).isEqualTo(GLOBAL_ENDPOINT);
        }

        LOG.info("[{}] connected to nodes {} with load balancing policy {}",
            Json.toJson(session.getName()),
            Json.toJson(nodes),
            loadBalancingPolicy);

        return session;
    }

    @NonNull
    private CqlSession connect(@NonNull final DriverConfigLoader configLoader, final boolean multiRegionWrites)
        throws InterruptedException {

        final CqlSession session = CqlSession.builder().withConfigLoader(checkState(configLoader)).build();

        try {
            Thread.sleep(2_000L); // Gives the session time to enumerate peers and initialize all channel pools
            return checkState(session, multiRegionWrites);
        } catch (final Throwable error) {
            session.close();
            throw error;
        }
    }

    private static ProgrammaticDriverConfigLoaderBuilder newProgrammaticDriverConfigLoaderBuilder() {
        return DriverConfigLoader.programmaticBuilder().withClass(
            DefaultDriverOption.RETRY_POLICY_CLASS,
            DefaultRetryPolicy.class);
    }

    private void testAllStatements(@NonNull final CqlSession session, @NonNull final String keyspaceName) {

        final String tableName = "sensor_data";

        try {

            assertThatCode(() ->
                TestCommon.createSchema(session, keyspaceName, tableName)
            ).doesNotThrowAnyException();

            // SimpleStatements

            session.execute(SimpleStatement.newInstance(format(
                "SELECT * FROM %s.%s WHERE sensor_id=uuid() and date=toDate(now())",
                keyspaceName,
                tableName)));

            session.execute(SimpleStatement.newInstance(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
                keyspaceName,
                tableName)));

            session.execute(SimpleStatement.newInstance(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp=toTimestamp("
                    + "now())",
                keyspaceName,
                tableName)));

            session.execute(SimpleStatement.newInstance(format(
                "DELETE FROM %s.%s WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp=toTimestamp(now())",
                keyspaceName,
                tableName)));

            // Built statements

            final LocalDate date = LocalDate.of(2016, 6, 30);
            final Instant timestamp = Instant.now();
            final UUID uuid = UUID.randomUUID();

            final Select select = QueryBuilder.selectFrom(keyspaceName, tableName)
                .all()
                .whereColumn("sensor_id").isEqualTo(literal(uuid))
                .whereColumn("date").isEqualTo(literal(date))
                .whereColumn("timestamp").isEqualTo(literal(timestamp));

            session.execute(select.build());

            final Insert insert = QueryBuilder.insertInto(keyspaceName, tableName)
                .value("sensor_id", literal(uuid))
                .value("date", literal(date))
                .value("timestamp", literal(timestamp));

            session.execute(insert.build());

            final Update update = QueryBuilder.update(keyspaceName, tableName)
                .setColumn("value", literal(1.0))
                .whereColumn("sensor_id").isEqualTo(literal(uuid))
                .whereColumn("date").isEqualTo(literal(date))
                .whereColumn("timestamp").isEqualTo(literal(timestamp));

            session.execute(update.build());

            final Delete delete = QueryBuilder.deleteFrom(keyspaceName, tableName)
                .whereColumn("sensor_id").isEqualTo(literal(uuid))
                .whereColumn("date").isEqualTo(literal(date))
                .whereColumn("timestamp").isEqualTo(literal(timestamp));

            session.execute(delete.build());

            // BoundStatements

            PreparedStatement preparedStatement = session.prepare(format(
                "SELECT * FROM %s.%s WHERE sensor_id = ? and date = ?",
                keyspaceName,
                tableName));

            BoundStatement boundStatement = preparedStatement.bind(uuid, date);
            session.execute(boundStatement);

            preparedStatement = session.prepare(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (?, ?, ?)",
                keyspaceName,
                tableName));

            boundStatement = preparedStatement.bind(uuid, date, timestamp);
            session.execute(boundStatement);

            preparedStatement = session.prepare(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = ? AND date = ? AND timestamp = ?",
                keyspaceName,
                tableName));

            boundStatement = preparedStatement.bind(uuid, date, timestamp);
            session.execute(boundStatement);

            preparedStatement = session.prepare(format(
                "DELETE FROM %s.%s WHERE sensor_id = ? AND date = ? AND timestamp = ?",
                keyspaceName,
                tableName));

            boundStatement = preparedStatement.bind(uuid, date, timestamp);
            session.execute(boundStatement);

            // BatchStatement (NOTE: BATCH requests must be single table Update/Delete/Insert statements)

            final BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED)
                .add(boundStatement)
                .add(boundStatement);

            session.execute(batchStatement);

        } finally {
            session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
        }
    }

    // endregion
}
