// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PORT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.READ_DATACENTER;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.WRITE_DATACENTER;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.cleanUp;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.createSchema;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.uniqueName;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test illustrates use of the {@link CosmosLoadBalancingPolicy} class.
 * <h3>
 * Preconditions:
 * <ol>
 * <li>A Cosmos DB Cassandra API account is required.
 * <li>These system variables or--alternatively--environment variables must be set.
 * <table>
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
 * Side effects:
 * <ol>
 * <li>Creates a number of keyspaces in the cluster, each with replication factor 3. To prevent collisions especially
 * during CI test runs, we generate keyspace names of the form <i><name></i><b>_</b><i></i><random-uuid></i>. Should a
 * keyspace by the generated name already exists, it is reused.
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

    private static final int TIMEOUT = 300000;

    // endregion

    // region Methods

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testGlobalAndReadDC() {
        final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
            .withGlobalEndpoint(GLOBAL_ENDPOINT)
            .withReadDC(READ_DATACENTER)
            .build();
        this.testAllStatements(this.connect(policy), uniqueName("globalAndRead"));
    }

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testGlobalEndpointOnly() {
        final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
            .withGlobalEndpoint(GLOBAL_ENDPOINT)
            .build();
        this.testAllStatements(this.connect(policy), uniqueName("globalOnly"));
    }

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testInvalid() {

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().withReadDC(READ_DATACENTER).build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().withWriteDC(WRITE_DATACENTER).build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(GLOBAL_ENDPOINT).withWriteDC(WRITE_DATACENTER).build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(GLOBAL_ENDPOINT)
                .withReadDC(READ_DATACENTER)
                .withWriteDC(WRITE_DATACENTER)
                .build()
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testReadAndWrite() {
        if (!GLOBAL_ENDPOINT.isEmpty()) {
            final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
                .withReadDC(READ_DATACENTER)
                .withWriteDC(WRITE_DATACENTER)
                .build();
            this.testAllStatements(this.connect(policy), uniqueName("readWriteDCv2"));
        }
    }

    // endregion

    // region Privates

    private Session connect(final LoadBalancingPolicy loadBalancingPolicy) {

        final Cluster cluster = Cluster.builder()
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoints(CONTACT_POINTS)
            .withPort(PORT)
            .withSSL()
            .build();

        try {
            return cluster.connect();
        } catch (final Throwable error) {
            cluster.close();
            throw error;
        }
    }

    private void testAllStatements(final Session session, final String keyspaceName) {

        final String tableName = "sensor_data";

        try {
            assertThatCode(() -> createSchema(session, keyspaceName, tableName)).doesNotThrowAnyException();

            // SimpleStatements

            SimpleStatement simpleStatement = new SimpleStatement(format(
                "SELECT * FROM %s.%s WHERE sensor_id = uuid() and date = toDate(now())",
                keyspaceName,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
                keyspaceName,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = "
                    + "toTimestamp(now())",
                keyspaceName,
                tableName));
            session.execute(simpleStatement);

            simpleStatement = new SimpleStatement(format(
                "DELETE FROM %s.%s WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())",
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
