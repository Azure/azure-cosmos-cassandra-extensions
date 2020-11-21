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
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.PORT;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.microsoft.azure.cosmos.cassandra.TestCommon.createSchema;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test illustrates use of the {@link CosmosLoadBalancingPolicy} class.
 *
 * <p>Preconditions:
 * <ul>
 * <li> A CosmosDB CassandraAPI account is required. It should have two regions: readDC (e.g, East US 2)
 * and writeDC (e.g, West US 2). globalEndpoint, username, and password fields should be populated.
 * <p>
 * <p>
 * Side effects:
 * <ol>
 * <li>Creates a new keyspace {@code keyspaceName} in the cluster, with replication factor 3. If a
 * keyspace with this name already exists, it will be reused;
 * <li>Creates a new table {@code keyspaceName.tableName}. If a table with that name exists
 * already, it will be reused.
 * <li>Executes all types of Statement queries.
 * </ol>
 * <p>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosLoadBalancingPolicyTest {

    // region Fields

    private static final int TIMEOUT = 300000;
    public String readDC = "East US 2";
    public String writeDC = "West US 2";
    private Cluster cluster;
    private String keyspaceName;
    private Session session;

    // endregion

    // region Methods

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testGlobalAndReadDC() {
        if (!GLOBAL_ENDPOINT.isEmpty()) {
            final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(GLOBAL_ENDPOINT)
                .withReadDC(this.readDC)
                .build();
            this.connect(policy);
            this.keyspaceName = "globalAndRead";
            this.testAllStatements();
        }
    }

    @AfterTest
    public void cleanUp() {
        if (this.session != null && !this.session.isClosed()) {
            this.session.execute(format("DROP KEYSPACE IF EXISTS %s", this.keyspaceName));
            this.close();
        }
    }

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testGlobalEndpointOnly() {
        if (!GLOBAL_ENDPOINT.isEmpty()) {
            this.keyspaceName = "globalOnly";
            final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(GLOBAL_ENDPOINT)
                .build();
            this.connect(policy);
            this.testAllStatements();
        }
    }

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testInvalid() {

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().withReadDC(this.readDC).build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().withWriteDC(this.writeDC).build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(GLOBAL_ENDPOINT).withWriteDC(this.writeDC).build()
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
            CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(GLOBAL_ENDPOINT)
                .withReadDC(this.readDC)
                .withWriteDC(this.writeDC)
                .build()
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test(groups = { "integration", "checkintest" }, timeOut = TIMEOUT)
    public void testReadAndWrite() {
        if (!GLOBAL_ENDPOINT.isEmpty()) {

            final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
                .withReadDC(this.readDC)
                .withWriteDC(this.writeDC)
                .build();

            this.keyspaceName = "readWriteDCv2";
            this.connect(policy);
            this.testAllStatements();
        }
    }

    // endregion

    // region Privates

    /**
     * Closes the session and the cluster.
     */
    private void close() {
        if (this.session != null) {
            this.session.close();
            this.cluster.close();
        }
    }

    private void connect(final LoadBalancingPolicy loadBalancingPolicy) {

        this.cluster = Cluster.builder()
            .withLoadBalancingPolicy(loadBalancingPolicy)  // under test
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoints(CONTACT_POINTS)
            .withPort(PORT)
            .withSSL()
            .build();

        System.out.println("Connected to cluster: " + this.cluster.getClusterName());
        this.session = this.cluster.connect();
    }

    private void testAllStatements() {

        final String tableName = "sensor_data";

        assertThatCode(() -> createSchema(this.session, this.keyspaceName, tableName)).doesNotThrowAnyException();

        // SimpleStatements

        SimpleStatement simpleStatement = new SimpleStatement(format(
            "SELECT * FROM %s.%s WHERE sensor_id = uuid() and date = toDate(now())",
            this.keyspaceName,
            tableName));
        this.session.execute(simpleStatement);

        simpleStatement = new SimpleStatement(format(
            "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
            this.keyspaceName,
            tableName));
        this.session.execute(simpleStatement);

        simpleStatement = new SimpleStatement(format(
            "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = "
                + "toTimestamp(now())",
            this.keyspaceName,
            tableName));
        this.session.execute(simpleStatement);

        simpleStatement = new SimpleStatement(format(
            "DELETE FROM %s.%s WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())",
            this.keyspaceName,
            tableName));
        this.session.execute(simpleStatement);

        // BuiltStatements

        final UUID uuid = UUID.randomUUID();
        final LocalDate date = LocalDate.fromYearMonthDay(2016, 06, 30);
        final Date timestamp = new Date();

        final Clause pk1Clause = QueryBuilder.eq("sensor_id", uuid);
        final Clause pk2Clause = QueryBuilder.eq("date", date);
        final Clause ckClause = QueryBuilder.eq("timestamp", 1000);

        final Select select = QueryBuilder.select()
            .all()
            .from(this.keyspaceName, tableName);

        select.where(pk1Clause).and(pk2Clause).and(ckClause);
        this.session.execute(select);

        final Insert insert = QueryBuilder.insertInto(this.keyspaceName, tableName);
        insert.values(new String[] { "sensor_id", "date", "timestamp" }, new Object[] { uuid, date, 1000 });
        this.session.execute(insert);

        final Update update = QueryBuilder.update(this.keyspaceName, tableName);
        update.with(set("value", 1.0)).where(pk1Clause).and(pk2Clause).and(ckClause);
        this.session.execute(update);

        final Delete delete = QueryBuilder.delete().from(this.keyspaceName, tableName);
        delete.where(pk1Clause).and(pk2Clause).and(ckClause);
        this.session.execute(delete);

        // BoundStatements

        PreparedStatement preparedStatement = this.session.prepare(format(
            "SELECT * FROM %s.%s WHERE sensor_id = ? and date = ?",
            this.keyspaceName,
            tableName));
        BoundStatement boundStatement = preparedStatement.bind(uuid, date);
        this.session.execute(boundStatement);

        preparedStatement = this.session.prepare(format(
            "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (?, ?, ?)",
            this.keyspaceName,
            tableName));
        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        this.session.execute(boundStatement);

        preparedStatement = this.session.prepare(format(
            "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = ? AND date = ? AND timestamp = ?",
            this.keyspaceName,
            tableName));
        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        this.session.execute(boundStatement);

        preparedStatement = this.session.prepare(format(
            "DELETE FROM %s.%s WHERE sensor_id = ? AND date = ? AND timestamp = ?",
            this.keyspaceName,
            tableName));
        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        this.session.execute(boundStatement);

        // BatchStatement

        final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED)
            .add(simpleStatement)
            .add(boundStatement);
        this.session.execute(batchStatement);
    }

    // endregion
}
