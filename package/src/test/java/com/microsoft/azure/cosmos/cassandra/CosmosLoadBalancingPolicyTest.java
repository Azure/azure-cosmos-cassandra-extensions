/*
 * The MIT License (MIT)
 *
 * Copyright (c) Microsoft. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
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
import java.util.Map;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.fail;

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
public class CosmosLoadBalancingPolicyTest {

    public String hostname = "<FILL ME>";
    public String username = "<FILL ME>";
    public String password = "<FILL ME>";
    public int port = 10350;
    public String readDC = "East US 2";
    public String writeDC = "West US 2";

    @AfterTest
    public void cleanUp() {
        if (session != null) {
            session.execute(format("DROP KEYSPACE IF EXISTS %s", keyspaceName));
        }

        this.close();
    }

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestInvalid() {
        try {
            CosmosLoadBalancingPolicy.builder().build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withReadDC(readDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withWriteDC(writeDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(hostname).withWriteDC(writeDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(hostname).withReadDC(readDC).withWriteDC(writeDC).build();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestGlobalEndpointOnly() {
        if (hostname != "<FILL ME>") {
            keyspaceName = "globalOnly";
            LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(hostname).build();
            this.connectWithSslAndLoadBalancingPolicy(policy);
            TestAllStatements();
        }
    }

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestGlobalAndReadDC() {
        if (hostname != "<FILL ME>") {
            keyspaceName = "globalAndRead";
            LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(hostname).withReadDC(readDC).build();
            this.connectWithSslAndLoadBalancingPolicy(policy);
            TestAllStatements();
        }
    }

    @Test(groups = {"integration", "checkin"}, timeOut = TIMEOUT)
    public void TestReadAndWrite() {
        if (hostname != "<FILL ME>") {
            keyspaceName = "readWriteDCv2";
            LoadBalancingPolicy policy = CosmosLoadBalancingPolicy.builder().withReadDC(readDC).withWriteDC(writeDC).build();
            this.connectWithSslAndLoadBalancingPolicy(policy);
            TestAllStatements();
        }
    }

    private void TestAllStatements() {

        try {
            TestCommon.createSchema(session, keyspaceName, tableName);
        } catch (Throwable error) {
            fail(format("createSchema failed: %s", error));
        }

        // SimpleStatements
        SimpleStatement simpleStatement = SimpleStatement.newInstance(format("SELECT * FROM %s.%s WHERE sensor_id = uuid() and date = toDate(now())", keyspaceName, tableName));
        session.execute(simpleStatement);

        simpleStatement = SimpleStatement.newInstance(format("INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));", keyspaceName, tableName));
        session.execute(simpleStatement);

        simpleStatement = SimpleStatement.newInstance(format("UPDATE %s.%s SET value = 1.0 WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())", keyspaceName, tableName));
        session.execute(simpleStatement);

        simpleStatement = SimpleStatement.newInstance(format("DELETE FROM %s.%s WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())", keyspaceName, tableName));
        session.execute(simpleStatement);

        // BuiltStatements

        final LocalDate date = LocalDate.of(2016, 06, 30);
        final Instant timestamp = Instant.now();
        final UUID uuid = UUID.randomUUID();

        Relation relation = Relation.columns("sensor_id", "date", "timestamp").isEqualTo(tuple(
            literal(uuid), literal(date), literal(timestamp)
        ));

        Select select = QueryBuilder.selectFrom(keyspaceName, tableName).all().where(relation);
        session.execute(select.build());

        Insert insert = QueryBuilder.insertInto(keyspaceName, tableName)
            .value("sensor_data", literal(uuid))
            .value("date", literal(date))
            .value("timestamp", literal(1000));

        session.execute(insert.build());

        Update update = QueryBuilder.update(keyspaceName, tableName)
            .setColumn("value", literal(1.0))
            .where(relation);

        session.execute(update.build());

        Delete delete = QueryBuilder.deleteFrom(keyspaceName, tableName).where(relation);
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

        // BatchStatement

        BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED)
            .add(simpleStatement)
            .add(boundStatement);

        session.execute(batchStatement);
    }

    private CqlSession session;
    private String keyspaceName = "downgrading";
    private String tableName = "sensor_data";

    private static final int TIMEOUT = 300_000;

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
            .withAuthCredentials(username, password)
            .withSslContext(sslContext)
            .build();

        System.out.println("Connected to session: " + session.getName());
        return session;
    }

    /**
     * Closes the session and the cluster.
     */
    private void close() {
        if (session != null) {
            session.close();
        }
    }
}
