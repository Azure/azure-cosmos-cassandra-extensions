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

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static org.assertj.core.api.Assertions.fail;

/**
 * This test illustrates use of the {@link CosmosDBLoadBalancingPolicy} class.
 *
 * <p>Preconditions:
 * <ul>
 * <li> A CosmosDB CassandraAPI account is required. It should have two regions: readDC (e.g, East US 2)
 * and writeDC (e.g, West US 2). globalEndpoint, username, and password fields should be populated.
 * <p>
 *
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
public class CosmosDBLoadBalancingPolicyTest {

    public String globalEndpoint = "<FILLME>";
    public String username = "<FILLME>";
    public String password = "<FILLME>";
    public int port = 10350;
    public String readDC = "East US 2";
    public String writeDC = "West US 2";

    @AfterTest
    public void cleanUp() {
        if (session != null) {
            session.execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspaceName));
        }

        this.close();
    }

    @Test(groups = {"integration", "checkintest"}, timeOut = TIMEOUT)
    public void TestInvalid() {
        try {
            CosmosDBLoadBalancingPolicy.builder().build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosDBLoadBalancingPolicy.builder().withReadDC(readDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosDBLoadBalancingPolicy.builder().withWriteDC(writeDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosDBLoadBalancingPolicy.builder().withGlobalEndpoint(globalEndpoint).withWriteDC(writeDC).build();
        } catch (IllegalArgumentException e) {
        }

        try {
            CosmosDBLoadBalancingPolicy.builder().withGlobalEndpoint(globalEndpoint).withReadDC(readDC).withWriteDC(writeDC).build();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test(groups = {"integration", "checkintest"}, timeOut = TIMEOUT)
    public void TestGlobalEndpointOnly() {
        keyspaceName = "globalOnly";
        LoadBalancingPolicy policy = CosmosDBLoadBalancingPolicy.builder().withGlobalEndpoint(globalEndpoint).build();
        this.connectWithSslAndLoadBalancingPolicy(policy);
        TestAllStatements();
    }

    @Test(groups = {"integration", "checkintest"}, timeOut = TIMEOUT)
    public void TestGlobalAndReadDC() {
        keyspaceName = "globalAndRead";
        LoadBalancingPolicy policy = CosmosDBLoadBalancingPolicy.builder().withGlobalEndpoint(globalEndpoint).withReadDC(readDC).build();
        this.connectWithSslAndLoadBalancingPolicy(policy);
        TestAllStatements();
    }

    @Test(groups = {"integration", "checkintest"}, timeOut = TIMEOUT)
    public void TestReadAndWrite() {
        keyspaceName = "readWriteDCv2";
        LoadBalancingPolicy policy = CosmosDBLoadBalancingPolicy.builder().withReadDC(readDC).withWriteDC(writeDC).build();
        this.connectWithSslAndLoadBalancingPolicy(policy);
        TestAllStatements();
    }

    private void TestAllStatements() {
        try {
            TestCommon.createSchema(session, keyspaceName, tableName);
        } catch (Exception error) {
            fail(String.format("createSchema failed: %s", error));
        }

        // SimpleStatements
        SimpleStatement simpleStatement = new SimpleStatement(String.format("SELECT * FROM %s.%s WHERE sensor_id = uuid() and date = toDate(now())", keyspaceName, tableName));
        session.execute(simpleStatement);

        simpleStatement = new SimpleStatement(String.format("INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));", keyspaceName, tableName));
        session.execute(simpleStatement);

        simpleStatement = new SimpleStatement(String.format("UPDATE %s.%s SET value = 1.0 WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())", keyspaceName, tableName));
        session.execute(simpleStatement);

        simpleStatement = new SimpleStatement(String.format("DELETE FROM %s.%s WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())", keyspaceName, tableName));
        session.execute(simpleStatement);

        // BuiltStatements
        UUID uuid = UUID.randomUUID();
        LocalDate date = LocalDate.fromYearMonthDay(2016, 06, 30);
        Date timestamp = new Date();

        Clause pk1Clause = QueryBuilder.eq("sensor_id", uuid);
        Clause pk2Clause = QueryBuilder.eq("date", date);
        Clause ckClause = QueryBuilder.eq("timestamp", 1000);
        Select select = QueryBuilder.select().all().from(keyspaceName, tableName);
        select.where(pk1Clause).and(pk2Clause).and(ckClause);
        session.execute(select);

        Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
        insert.values(new String[] { "sensor_id", "date", "timestamp" }, new Object[] { uuid, date, 1000 });
        session.execute(insert);

        Update update = QueryBuilder.update(keyspaceName, tableName);
        update.with(set("value", 1.0)).where(pk1Clause).and(pk2Clause).and(ckClause);
        session.execute(update);

        Delete delete = QueryBuilder.delete().from(keyspaceName, tableName);
        delete.where(pk1Clause).and(pk2Clause).and(ckClause);
        session.execute(delete);

        // BoundStatements
        PreparedStatement preparedStatement = session.prepare(String.format("SELECT * FROM %s.%s WHERE sensor_id = ? and date = ?", keyspaceName, tableName));
        BoundStatement boundStatement = preparedStatement.bind(uuid, date);
        session.execute(boundStatement);

        preparedStatement = session.prepare(String.format("INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (?, ?, ?)", keyspaceName, tableName));
        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        session.execute(boundStatement);

        preparedStatement = session.prepare(String.format("UPDATE %s.%s SET value = 1.0 WHERE sensor_id = ? AND date = ? AND timestamp = ?", keyspaceName, tableName));
        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        session.execute(boundStatement);

        preparedStatement = session.prepare(String.format("DELETE FROM %s.%s WHERE sensor_id = ? AND date = ? AND timestamp = ?", keyspaceName, tableName));
        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        session.execute(boundStatement);

        // BatchStatement
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        batchStatement.add(simpleStatement);
        batchStatement.add(boundStatement);
        session.execute(batchStatement);
    }

    private Cluster cluster;
    private Session session;
    private String keyspaceName = "downgrading";
    private String tableName = "sensor_data";

    private static final int TIMEOUT = 300000;

    private void connectWithSslAndLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
        cluster = Cluster.builder().addContactPoints(globalEndpoint).withPort(port).withCredentials(username, password).withSSL().withLoadBalancingPolicy(loadBalancingPolicy).build();
        System.out.println("Connected to cluster: " + cluster.getClusterName());
        session = cluster.connect();
    }

    /**
     * Closes the session and the cluster.
     */
    private void close() {
        if (session != null) {
            session.close();
            cluster.close();
        }
    }
}
