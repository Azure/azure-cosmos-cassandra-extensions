// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.PORT;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.getPropertyOrEnvironmentVariable;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

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

    private static final String GLOBAL_ENDPOINT = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.globalEndpoint",
        "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
        "");

    private static final String READ_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.readDatacenter",
        "AZURE_COSMOS_CASSANDRA_READ_DATACENTER",
        "localhost");

    private static final String WRITE_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.writeDatacenter",
        "AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER",
        "localhost");

    private final String tableName = "sensor_data";
    private String keyspaceName = "downgrading";
    private CqlSession session;

    // endregion

    // region Methods

    @AfterTest
    public void cleanUp() {
        if (this.session != null && !this.session.isClosed()) {
            this.session.execute(format("DROP KEYSPACE IF EXISTS %s", this.keyspaceName));
            this.close();
        }
    }

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        if (this.session != null) {
            this.session.close();
        }
    }

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testGlobalEndpointAndReadDatacenter() {

        if (GLOBAL_ENDPOINT != null) {

            this.keyspaceName = "globalAndRead";

            DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
                .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, READ_DATACENTER)
                .build();

            try (CqlSession ignored = this.connect(configLoader)) {
                this.testAllStatements();
            }
        }
    }

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testGlobalEndpointOnly() {

        if (GLOBAL_ENDPOINT != null) {

            this.keyspaceName = "globalOnly";

            final DriverConfigLoader driverConfigLoader = newProgrammaticDriverConfigLoaderBuilder()
                .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                .build();

            try (CqlSession ignored = this.connect(driverConfigLoader)) {
                this.testAllStatements();
            }
        }
    }

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testInvalidConfiguration() {

        final ProgrammaticArguments programmaticArguments = ProgrammaticArguments.builder().build();

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(newProgrammaticDriverConfigLoaderBuilder().build(), programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, READ_DATACENTER)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, WRITE_DATACENTER)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                    .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, WRITE_DATACENTER)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                    .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, READ_DATACENTER)
                    .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, WRITE_DATACENTER)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);
    }

    // endregion

    // region Privates

    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testReadAndWrite() {

        if (GLOBAL_ENDPOINT != null) {

            this.keyspaceName = "readWriteDCv2";

            DriverConfigLoader driverConfigLoader = newProgrammaticDriverConfigLoaderBuilder()
                .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, READ_DATACENTER)
                .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, WRITE_DATACENTER)
                .build();

            try (CqlSession ignored = this.connect(driverConfigLoader)) {
                this.testAllStatements();
            }
        }
    }

    private static ProgrammaticDriverConfigLoaderBuilder newProgrammaticDriverConfigLoaderBuilder() {
        return DriverConfigLoader.programmaticBuilder().withClass(
            DefaultDriverOption.RETRY_POLICY_CLASS,
            DefaultRetryPolicy.class);
    }

    private CqlSession connect(DriverConfigLoader configLoader) {

        final Collection<EndPoint> endpoints = Collections.singletonList(new DefaultEndPoint(new InetSocketAddress(
            GLOBAL_ENDPOINT,
            PORT)));

        this.session = CqlSession.builder()
            .withAuthCredentials(USERNAME, PASSWORD)
            .withConfigLoader(configLoader)
            .addContactEndPoints(endpoints)
            .build();

        System.out.println("Connected to session: " + this.session.getName());
        return this.session;
    }

    private void testAllStatements() {

        assertThatCode(() ->
            TestCommon.createSchema(this.session, this.keyspaceName, this.tableName)
        ).doesNotThrowAnyException();

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

        // Built statements

        final LocalDate date = LocalDate.of(2016, 6, 30);
        final Instant timestamp = Instant.now();
        final UUID uuid = UUID.randomUUID();

        Select select = QueryBuilder.selectFrom(this.keyspaceName, this.tableName)
            .all()
            .whereColumn("sensor_id").isEqualTo(literal(uuid))
            .whereColumn("date").isEqualTo(literal(date))
            .whereColumn("timestamp").isEqualTo(literal(timestamp));

        this.session.execute(select.build());

        Insert insert = QueryBuilder.insertInto(this.keyspaceName, this.tableName)
            .value("sensor_id", literal(uuid))
            .value("date", literal(date))
            .value("timestamp", literal(timestamp));

        this.session.execute(insert.build());

        Update update = QueryBuilder.update(this.keyspaceName, this.tableName)
            .setColumn("value", literal(1.0))
            .whereColumn("sensor_id").isEqualTo(literal(uuid))
            .whereColumn("date").isEqualTo(literal(date))
            .whereColumn("timestamp").isEqualTo(literal(timestamp));

        this.session.execute(update.build());

        Delete delete = QueryBuilder.deleteFrom(this.keyspaceName, this.tableName)
            .whereColumn("sensor_id").isEqualTo(literal(uuid))
            .whereColumn("date").isEqualTo(literal(date))
            .whereColumn("timestamp").isEqualTo(literal(timestamp));

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

        // BatchStatement (NOTE: BATCH requests must be single table Update/Delete/Insert statements)

        BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED)
            .add(boundStatement)
            .add(boundStatement);

        this.session.execute(batchStatement);
    }

    // endregion
}
