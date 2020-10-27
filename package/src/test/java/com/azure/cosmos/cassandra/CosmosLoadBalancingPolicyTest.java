// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/**
 * This test illustrates use of the {@link CosmosLoadBalancingPolicy} class.
 * <p>
 * Preconditions:
 * <ul>
 * <li> A CosmosDB CassandraAPI account is required. It should have two regions: readDC (e.g, East US 2)
 * and writeDC (e.g, West US 2). globalEndpoint, username, and password fields should be populated.
 * <p>
 * Side effects:
 * <ol>
 * <li>Creates a new keyspace {@code keyspaceName} in the cluster, with replication factor 3. If a
 * keyspace with this name already exists, it will be reused;
 * <li>Creates a new table {@code keyspaceName.tableName}. If a table with that name exists
 * already, it will be reused.
 * <li>Executes all types of Statement queries.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosLoadBalancingPolicyTest implements AutoCloseable {

    // region Fields

    private static final int TIMEOUT = 300_000;
    public final String hostname = "<FILL ME>";
    public final String password = "<FILL ME>";
    public final int port = 10350;
    public final String readDC = "East US 2";
    public final String username = "<FILL ME>";
    public final String writeDC = "West US 2";
    private final String tableName = "sensor_data";
    private String keyspaceName = "downgrading";
    private CqlSession session;

    // endregion

    // region Methods

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestGlobalAndReadDC() {

        if (this.hostname != "<FILL ME>") {

            this.keyspaceName = "globalAndRead";

            LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(this.hostname)
                .withReadDC(this.readDC)
                .build();

            try (CqlSession ignored = this.connectWithSslAndLoadBalancingPolicy(policy)) {
                this.TestAllStatements();
            }
        }
    }

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestGlobalEndpointOnly() {
        if (this.hostname != "<FILL ME>") {
            this.keyspaceName = "globalOnly";
            LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(this.hostname).build();
            this.connectWithSslAndLoadBalancingPolicy(policy);
            this.TestAllStatements();
        }
    }

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void TestInvalid() {

        try {
            CosmosLoadBalancingPolicy.builder().build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withReadDC(this.readDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withWriteDC(this.writeDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(this.hostname).withWriteDC(this.writeDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(this.hostname).withReadDC(this.readDC).withWriteDC(this.writeDC).build();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestReadAndWrite() {

        if (this.hostname != "<FILL ME>") {

            this.keyspaceName = "readWriteDCv2";

            final LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder()
                .withReadDC(this.readDC)
                .withWriteDC(this.writeDC)
                .build();

            try (CqlSession ignored = this.connectWithSslAndLoadBalancingPolicy(policy)) {
                this.TestAllStatements();
            }
        }
    }

    @AfterTest
    public void cleanUp() {
        if (this.session != null) {
            this.session.execute(format("DROP KEYSPACE IF EXISTS %s", this.keyspaceName));
        }

        this.close();
    }

    // endregion

    // region Privates

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        if (this.session != null) {
            this.session.close();
        }
    }

    private void TestAllStatements() {

        assertThatCode(() -> TestCommon.createSchema(this.session, this.keyspaceName, this.tableName))
            .doesNotThrowAnyException();

        // SimpleStatements

        this.session.execute(SimpleStatement.newInstance(format(
            "SELECT * FROM %s.%s WHERE sensor_id = uuid() and date = toDate(now())",
            this.keyspaceName,
            this.tableName)));

        this.session.execute(SimpleStatement.newInstance(format(
            "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
            this.keyspaceName,
            this.tableName)));

        this.session.execute(SimpleStatement.newInstance(format(
            "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = "
                + "toTimestamp(now())",
            this.keyspaceName,
            this.tableName)));

        this.session.execute(SimpleStatement.newInstance(format(
            "DELETE FROM %s.%s WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())",
            this.keyspaceName,
            this.tableName)));

        // BuiltStatements

        final LocalDate date = LocalDate.of(2016, 6, 30);
        final Instant timestamp = Instant.now();
        final UUID uuid = UUID.randomUUID();

        Relation relation = Relation.columns("sensor_id", "date", "timestamp").isEqualTo(tuple(
            literal(uuid), literal(date), literal(timestamp)
        ));

        Select select = QueryBuilder.selectFrom(this.keyspaceName, this.tableName).all().where(relation);
        this.session.execute(select.build());

        Insert insert = QueryBuilder.insertInto(this.keyspaceName, this.tableName)
            .value("sensor_data", literal(uuid))
            .value("date", literal(date))
            .value("timestamp", literal(1000));

        this.session.execute(insert.build());

        Update update = QueryBuilder.update(this.keyspaceName, this.tableName)
            .setColumn("value", literal(1.0))
            .where(relation);

        this.session.execute(update.build());

        Delete delete = QueryBuilder.deleteFrom(this.keyspaceName, this.tableName).where(relation);
        this.session.execute(delete.build());

        // BoundStatements

        PreparedStatement preparedStatement = this.session.prepare(format(
            "SELECT * FROM %s.%s WHERE sensor_id = ? and date = ?",
            this.keyspaceName,
            this.tableName));

        BoundStatement boundStatement = preparedStatement.bind(uuid, date);
        this.session.execute(boundStatement);

        preparedStatement = this.session.prepare(format(
            "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (?, ?, ?)",
            this.keyspaceName,
            this.tableName));

        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        this.session.execute(boundStatement);

        preparedStatement = this.session.prepare(format(
            "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = ? AND date = ? AND timestamp = ?",
            this.keyspaceName,
            this.tableName));

        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        this.session.execute(boundStatement);

        preparedStatement = this.session.prepare(format(
            "DELETE FROM %s.%s WHERE sensor_id = ? AND date = ? AND timestamp = ?",
            this.keyspaceName,
            this.tableName));

        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        this.session.execute(boundStatement);

        // BatchStatement

        BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED)
            .add(SimpleStatement.newInstance(format(
                "SELECT * FROM %s.%s WHERE WHERE sensor_id = uuid() and date = toDate(now())",
                this.keyspaceName,
                this.tableName)))
            .add(boundStatement);

        this.session.execute(batchStatement);
    }

    private CqlSession connectWithSslAndLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {

        final Collection<EndPoint> endpoints = Collections.singletonList(new DefaultEndPoint(new InetSocketAddress(
            this.hostname,
            this.port)));

        final SSLContext sslContext;

        try {
            sslContext = SSLContext.getDefault();
        } catch (Throwable error) {
            fail("could not obtain the default SSL context due to " + error.getClass().getName());
            return null;
        }

        this.session = CqlSession.builder()
            .addContactEndPoints(endpoints)
            .withAuthCredentials(this.username, this.password)
            .withSslContext(sslContext)
            .build();

        System.out.println("Connected to session: " + this.session.getName());
        return this.session;
    }

    // endregion
}
