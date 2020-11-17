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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.regex.Matcher;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.getPropertyOrEnvironmentVariable;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
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
public class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final String READ_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.read-datacenter",
        "AZURE_COSMOS_CASSANDRA_READ_DATACENTER",
        "localhost");

    static final String WRITE_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.write-datacenter",
        "AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER",
        "localhost");

    private static final String TABLE_NAME = "sensor_data";
    private static final int TIMEOUT = 300_000;

    private String keyspaceName = "downgrading";

    // endregion

    // region Methods

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testGlobalEndpointAndReadDatacenter() {

        if (GLOBAL_ENDPOINT != null) {

            this.keyspaceName = "globalAndRead";

            DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
                .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, READ_DATACENTER)
                .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, "")
                .build();

            try (CqlSession session = this.connect(configLoader)) {
                try {
                    this.testAllStatements(session);
                } finally {
                    this.cleanUp(session);
                }
            }
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testGlobalEndpointOnly() {

        if (GLOBAL_ENDPOINT != null) {

            this.keyspaceName = "globalOnly";

            final DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
                .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, "")
                .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, "")
                .build();

            try (CqlSession session = this.connect(configLoader)) {
                try {
                    this.testAllStatements(session);
                } finally {
                    this.cleanUp(session);
                }
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

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT)
    public void testReadAndWrite() {

        if (GLOBAL_ENDPOINT != null) {

            this.keyspaceName = "readWriteDCv2";

            DriverConfigLoader driverConfigLoader = newProgrammaticDriverConfigLoaderBuilder()
                .withString(CosmosLoadBalancingPolicy.Option.GLOBAL_ENDPOINT, "")
                .withString(CosmosLoadBalancingPolicy.Option.READ_DATACENTER, READ_DATACENTER)
                .withString(CosmosLoadBalancingPolicy.Option.WRITE_DATACENTER, WRITE_DATACENTER)
                .build();

            try (CqlSession session = this.connect(driverConfigLoader)) {
                try {
                    this.testAllStatements(session);
                } finally {
                    this.cleanUp(session);
                }
            }
        }
    }

    // endregion

    // region Privates

    private void cleanUp(@NonNull CqlSession session) {
        session.execute(format("DROP KEYSPACE IF EXISTS %s", this.keyspaceName));
    }

    @NonNull
    private CqlSession connect(@NonNull DriverConfigLoader configLoader) {

        final Matcher address = TestCommon.HOSTNAME_AND_PORT.matcher(GLOBAL_ENDPOINT);
        assertThat(address.matches()).isTrue();

        final Collection<EndPoint> endPoints = Collections.singletonList(new DefaultEndPoint(new InetSocketAddress(
            address.group("hostname"),
            Integer.parseUnsignedInt(address.group("port")))));

        return CqlSession.builder()
            .withAuthCredentials(USERNAME, PASSWORD)
            .withConfigLoader(configLoader)
            .addContactEndPoints(endPoints)
            .build();
    }

    private static ProgrammaticDriverConfigLoaderBuilder newProgrammaticDriverConfigLoaderBuilder() {
        return DriverConfigLoader.programmaticBuilder().withClass(
            DefaultDriverOption.RETRY_POLICY_CLASS,
            DefaultRetryPolicy.class);
    }

    private void testAllStatements(CqlSession session) {

        assertThatCode(() ->
            TestCommon.createSchema(session, this.keyspaceName, this.TABLE_NAME)
        ).doesNotThrowAnyException();

        // SimpleStatements

        session.execute(SimpleStatement.newInstance(format(
            "SELECT * FROM %s.%s WHERE sensor_id = uuid() and date = toDate(now())",
            this.keyspaceName,
            this.TABLE_NAME)));

        session.execute(SimpleStatement.newInstance(format(
            "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (uuid(), toDate(now()), toTimestamp(now()));",
            this.keyspaceName,
            this.TABLE_NAME)));

        session.execute(SimpleStatement.newInstance(format(
            "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = "
                + "toTimestamp(now())",
            this.keyspaceName,
            this.TABLE_NAME)));

        session.execute(SimpleStatement.newInstance(format(
            "DELETE FROM %s.%s WHERE sensor_id = uuid() AND date = toDate(now()) AND timestamp = toTimestamp(now())",
            this.keyspaceName,
            this.TABLE_NAME)));

        // Built statements

        final LocalDate date = LocalDate.of(2016, 6, 30);
        final Instant timestamp = Instant.now();
        final UUID uuid = UUID.randomUUID();

        Select select = QueryBuilder.selectFrom(this.keyspaceName, this.TABLE_NAME)
            .all()
            .whereColumn("sensor_id").isEqualTo(literal(uuid))
            .whereColumn("date").isEqualTo(literal(date))
            .whereColumn("timestamp").isEqualTo(literal(timestamp));

        session.execute(select.build());

        Insert insert = QueryBuilder.insertInto(this.keyspaceName, this.TABLE_NAME)
            .value("sensor_id", literal(uuid))
            .value("date", literal(date))
            .value("timestamp", literal(timestamp));

        session.execute(insert.build());

        Update update = QueryBuilder.update(this.keyspaceName, this.TABLE_NAME)
            .setColumn("value", literal(1.0))
            .whereColumn("sensor_id").isEqualTo(literal(uuid))
            .whereColumn("date").isEqualTo(literal(date))
            .whereColumn("timestamp").isEqualTo(literal(timestamp));

        session.execute(update.build());

        Delete delete = QueryBuilder.deleteFrom(this.keyspaceName, this.TABLE_NAME)
            .whereColumn("sensor_id").isEqualTo(literal(uuid))
            .whereColumn("date").isEqualTo(literal(date))
            .whereColumn("timestamp").isEqualTo(literal(timestamp));

        session.execute(delete.build());

        // BoundStatements

        PreparedStatement preparedStatement = session.prepare(format(
            "SELECT * FROM %s.%s WHERE sensor_id = ? and date = ?",
            this.keyspaceName,
            this.TABLE_NAME));

        BoundStatement boundStatement = preparedStatement.bind(uuid, date);
        session.execute(boundStatement);

        preparedStatement = session.prepare(format(
            "INSERT INTO %s.%s (sensor_id, date, timestamp) VALUES (?, ?, ?)",
            this.keyspaceName,
            this.TABLE_NAME));

        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        session.execute(boundStatement);

        preparedStatement = session.prepare(format(
            "UPDATE %s.%s SET value = 1.0 WHERE sensor_id = ? AND date = ? AND timestamp = ?",
            this.keyspaceName,
            this.TABLE_NAME));

        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        session.execute(boundStatement);

        preparedStatement = session.prepare(format(
            "DELETE FROM %s.%s WHERE sensor_id = ? AND date = ? AND timestamp = ?",
            this.keyspaceName,
            this.TABLE_NAME));

        boundStatement = preparedStatement.bind(uuid, date, timestamp);
        session.execute(boundStatement);

        // BatchStatement (NOTE: BATCH requests must be single table Update/Delete/Insert statements)

        BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED)
            .add(boundStatement)
            .add(boundStatement);

        session.execute(batchStatement);
    }

    // endregion
}
