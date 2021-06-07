// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.buildCluster;
import static com.azure.cosmos.cassandra.TestCommon.cleanUp;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test illustrates use of the {@link CosmosLoadBalancingPolicy} class.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li> A Cosmos DB Cassandra API account is required. It should have two regions, one used as a read datacenter (e.g,
 * East US) and another used as a write datacenter (e.g., West US).
 * <li> These system or--alternatively--environment variables must be set.
 * <table><caption></caption>
 * <thead>
 * <tr>
 * <th>System variable</th>
 * <th>Environment variable</th>
 * <th>Description</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 * <td>azure.cosmos.cassandra.global-endpoint</td>
 * <td>AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT</td>
 * <td>Global endpoint address (e.g., "database-account.cassandra.cosmos.azure.com:10350")</td>
 * </tr>
 * <tr>
 * <td>azure.cosmos.cassandra.username</td>
 * <td>AZURE_COSMOS_CASSANDRA_USERNAME</td>
 * <td>Username for authentication</td>
 * </tr>
 * <tr>
 * <td>azure.cosmos.cassandra.password</td>
 * <td>AZURE_COSMOS_CASSANDRA_PASSWORD</td>
 * <td>Password for authentication</td>
 * </tr>
 * <tr>
 * <td>azure.cosmos.cassandra.read-datacenter</td>
 * <td>AZURE_COSMOS_CASSANDRA_READ_DATACENTER</td>
 * <td>Read datacenter name (e.g., "East US")</td>
 * </tr>
 * <tr>
 * <td>azure.cosmos.cassandra.write-datacenter</td>
 * <td>AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER</td>
 * <td>Write datacenter name (e.g., "West US")</td>
 * </tr>
 * </tbody>
 * </table>
 * </ol>
 * <h3>
 * Side effects</h3>
 * <ol>
 * <li>Creates a number of keyspaces in the cluster, each with replication factor 3. To prevent collisions especially
 * during CI test runs, we generate keyspace names of the form <i>&lt;name&gt;</i><b><code>_</code></b><i>&lt;
 * random-uuid&gt;</i>. Should a keyspace by the generated name already exists, it is reused.
 * <li>Creates a table within each keyspace created or reused. If a table with a given name already exists, it is
 * reused.
 * <li>Executes all types of {@link Statement} queries.
 * </li>The keyspaces created or reused are then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);
    private static final int TIMEOUT_IN_SECONDS = 3600;

    // endregion

    // region Methods

    @BeforeAll
    static void touchCosmosLoadBalancingPolicy() {
        CosmosLoadBalancingPolicy.builder(); // forces class initialization
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} without preferred regions issues routes requests correctly.
     * <p>
     * All requests should go to the global endpoint with failover to endpoints in other regions in alphabetic order. We
     * do not test the failover scenario here. We simply check that all requests are sent to the global endpoint whether
     * or not multi-region writes are enabled. Cosmos DB guarantees that both read and write requests can be sent to the
     * global endpoint.
     *
     * @param multiRegionWrites {@code true}, if the test should be run with multi-region writes enabled.
     */
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithoutPreferredRegions(final boolean multiRegionWrites) {

        try (Cluster cluster = buildCluster(
            "testWithoutPreferredRegions-multiRegionWrites-" + multiRegionWrites,
            CosmosLoadBalancingPolicy.builder()
                .withMultiRegionWrites(multiRegionWrites)
                .build())) {
            testAllStatements(cluster.connect(), TestCommon.uniqueName("TestWithoutPreferredRegions"));
        } catch (final AssertionError error) {
            throw error;
        } catch (final Throwable error) {
            fail("Failed due to: " + error, error);
        }
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} with preferred regions issues routes requests correctly.
     * <p>
     * The behavior varies based on whether multi-region writes are enabled. When multi-region writes are enabled all
     * requests (reads and writes) will be sent to the first region in the list of preferred regions with failover to
     * other regions in the preferred region list in the order in which they are listed. If all regions in the preferred
     * region list are down, all requests will fall back to the region in which the global endpoint is deployed, then to
     * other regions in alphabetic order.
     * <p>
     * When multi-region writes are disabled, all write requests are sent to the global-endpoint. There is no fallback.
     * Read requests are handled the same whether or not multi-region writes are enabled.
     * We do not test failover scenarios here. We simply check that all requests are sent to the first preferred region
     * when multi-region writes are enabled. When multi-region writes are disabled we verify that all read requests are
     * sent to the first preferred region and that all write requests are sent to the global endpoint address.
     *
     * @param multiRegionWrites {@code true}, if the test should be run with multi-region writes enabled.
     */
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithPreferredRegions(final boolean multiRegionWrites) {

        try (Cluster cluster = buildCluster(
            "testWithPreferredRegions-multiRegionWrites-" + multiRegionWrites,
            CosmosLoadBalancingPolicy.builder()
                .withMultiRegionWrites(multiRegionWrites)
                .withPreferredRegions(PREFERRED_REGIONS)
                .build())) {
            try {
                testAllStatements(cluster.connect(), TestCommon.uniqueName("testWithPreferredRegions"));
            } catch (final Throwable error) {
                LOG.error("{} failed due to: {}", cluster.getClusterName(), toJson(error));
                fail(toJson(error));
            }
        }
    }

    // endregion

    // region Privates

    private static void testAllStatements(final Session session, final String keyspaceName) {

        final String tableName = "sensor_data";

        try {
            assertThatCode(() -> TestCommon.createSchema(session, keyspaceName, tableName)).doesNotThrowAnyException();

            // SimpleStatements

            SimpleStatement simpleStatement = new SimpleStatement(format(
                "SELECT * FROM %s.%s WHERE sensor_id=uuid() and date=toDate(now())",
                keyspaceName,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
                keyspaceName,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp="
                    + "toTimestamp(now())",
                keyspaceName,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "DELETE FROM %s.%s WHERE sensor_id=uuid() AND date=toDate(now()) AND timestamp=toTimestamp(now())",
                keyspaceName,
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
                .from(keyspaceName, tableName);

            select.where(pk1Clause).and(pk2Clause).and(ckClause);
            session.execute(select);

            final Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
            insert.values(new String[] { "sensor_id", "date", "timestamp" }, new Object[] { uuid, date, 1000 });
            session.execute(insert);

            final Update update = QueryBuilder.update(keyspaceName, tableName);
            update.with(set("value", 1.0)).where(pk1Clause).and(pk2Clause).and(ckClause);
            session.execute(update);

            final Delete delete = QueryBuilder.delete().from(keyspaceName, tableName);
            delete.where(pk1Clause).and(pk2Clause).and(ckClause);
            session.execute(delete);

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

            // BatchStatement

            final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED)
                .add(simpleStatement)
                .add(boundStatement);
            session.execute(batchStatement);

        } finally {
            cleanUp(session, keyspaceName);
        }
    }

    // endregion
}
