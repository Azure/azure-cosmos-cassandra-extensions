// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.CosmosLoadBalancingPolicy.Option;
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
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

import static com.azure.cosmos.cassandra.TestCommon.CONTACT_POINTS;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.createSchema;
import static com.azure.cosmos.cassandra.TestCommon.getPropertyOrEnvironmentVariable;
import static com.azure.cosmos.cassandra.TestCommon.matchSocketAddress;
import static com.azure.cosmos.cassandra.TestCommon.uniqueName;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * during CI test runs, we generate keyspace names of the form <i>&lt;name&gt;</i><b>_</b><i>&lt;random-uuid&gt;</i>.
 * Should a keyspace by the generated name already exists, it is reused.
 * <li>Creates a table within each keyspace created or reused. If a table with a given name already exists, it is
 * reused.
 * <li>Executes all types of {@link Statement} queries.
 * <li>The keyspaces created or reused are then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public final class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);

    static final String READ_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.read-datacenter",
        "AZURE_COSMOS_CASSANDRA_READ_DATACENTER",
        "localhost");

    static final String WRITE_DATACENTER = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.write-datacenter",
        "AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER",
        "localhost");

    private static final int TIMEOUT_IN_MILLIS = 300_000;

    // endregion

    // region Methods

    @BeforeMethod
    public void logTestName(Method method) {
        LOG.info("{}", method.getName());
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} specifying the combination of a {@code read-datacenter} and a 
     * {@code global-endpoint} (with no {@code write-datacenter}) routes requests correctly.
     *
     * TODO (DANOBLE) Add the check that routing occurs as expected.
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT_IN_MILLIS)
    public void testGlobalEndpointAndReadDatacenter() {

        final DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
            .withString(Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
            .withString(Option.READ_DATACENTER, READ_DATACENTER)
            .withString(Option.WRITE_DATACENTER, "")
            .build();

        try (final CqlSession session = this.connect(configLoader)) {
            this.testAllStatements(session, uniqueName("globalAndRead"));
        }
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} specifying a {@code global-endpoint} (with no
     * {@code read-datacenter} or {@code write-datacenter}) routes requests correctly.
     *
     * TODO (DANOBLE) Add the check that routing occurs as expected.
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT_IN_MILLIS)
    public void testGlobalEndpointOnly() {

        final DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
            .withString(Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
            .withString(Option.READ_DATACENTER, "")
            .withString(Option.WRITE_DATACENTER, "")
            .build();

        try (final CqlSession session = this.connect(configLoader)) {
            this.testAllStatements(session, uniqueName("globalOnly"));
        }
    }

    /**
     * Verifies that invalid {@link CosmosLoadBalancingPolicy} configurations produce {@link IllegalArgumentException}
     * errors.
     */
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT_IN_MILLIS)
    public void testInvalidConfiguration() {

        final ProgrammaticArguments programmaticArguments = ProgrammaticArguments.builder().build();
        final String readDatacenter = "East US";
        final String writeDatacenter = "West US";

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(newProgrammaticDriverConfigLoaderBuilder().build(), programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(Option.READ_DATACENTER, readDatacenter)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(Option.WRITE_DATACENTER, writeDatacenter)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                    .withString(Option.WRITE_DATACENTER, writeDatacenter)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CosmosLoadBalancingPolicy(
            new DefaultDriverContext(
                newProgrammaticDriverConfigLoaderBuilder()
                    .withString(Option.GLOBAL_ENDPOINT, GLOBAL_ENDPOINT)
                    .withString(Option.READ_DATACENTER, readDatacenter)
                    .withString(Option.WRITE_DATACENTER, writeDatacenter)
                    .build(),
                programmaticArguments),
            "default")
        ).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} specifying a {@code read-datacenter} and a 
     * {@code write-datacenter} (with no {@code global-endpoint}) routes requests correctly.
     *
     * TODO (DANOBLE) Add the check that routing occurs as expected.
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration", "checkin" }, timeOut = TIMEOUT_IN_MILLIS)
    public void testReadDatacenterAndWriteDatacenter() {

        if (WRITE_DATACENTER.isEmpty()) {
            throw new SkipException("WRITE_DATACENTER is empty");
        }

        final DriverConfigLoader driverConfigLoader = newProgrammaticDriverConfigLoaderBuilder()
            .withString(Option.GLOBAL_ENDPOINT, "")
            .withString(Option.READ_DATACENTER, READ_DATACENTER)
            .withString(Option.WRITE_DATACENTER, WRITE_DATACENTER)
            .build();

        try (final CqlSession session = this.connect(driverConfigLoader)) {
            this.testAllStatements(session, uniqueName("readWriteDCv2"));
        }
    }

    // endregion

    // region Privates

    private static CqlSession checkState(final CqlSession session) {

        final DriverContext context = session.getContext();
        final DriverExecutionProfile profile = context.getConfig().getDefaultProfile();

        final RetryPolicy retryPolicy = context.getRetryPolicy(profile.getName());

        assertThat(retryPolicy.getClass()).isEqualTo(DefaultRetryPolicy.class);

        final LoadBalancingPolicy loadBalancingPolicy = context.getLoadBalancingPolicy(profile.getName());

        assertThat(loadBalancingPolicy.getClass()).isEqualTo(CosmosLoadBalancingPolicy.class);

        assertThat(((CosmosLoadBalancingPolicy) loadBalancingPolicy).getDnsExpiryTimeInSeconds())
            .isEqualTo(profile.getInt(Option.DNS_EXPIRY_TIME));

        final String globalEndpoint = profile.getString(Option.GLOBAL_ENDPOINT);

        if (!globalEndpoint.isEmpty()) {
            assertThat(((CosmosLoadBalancingPolicy) loadBalancingPolicy).getGlobalEndpoint())
                .isEqualTo(matchSocketAddress(globalEndpoint).group("hostname"));
        }

        assertThat(((CosmosLoadBalancingPolicy) loadBalancingPolicy).getReadDatacenter())
            .isEqualTo(profile.getString(Option.READ_DATACENTER));

        assertThat(((CosmosLoadBalancingPolicy) loadBalancingPolicy).getWriteDatacenter())
            .isEqualTo(profile.getString(Option.WRITE_DATACENTER));

        final Map<UUID, Node> nodes = session.getMetadata().getNodes();
        // TODO (DANOBLE) Add check that the number of nodes is correct based on a (to be defined) parameter to the test

        LOG.info("[{}] connected to {} with {} and {}",
            session.getName(),
            nodes,
            retryPolicy,
            loadBalancingPolicy);

        return session;
    }

    private void cleanUp(@NonNull final CqlSession session, @NonNull final String keyspaceName) {
        session.execute(format("DROP KEYSPACE IF EXISTS %s", keyspaceName));
    }

    @NonNull
    private CqlSession connect(@NonNull final DriverConfigLoader configLoader) {

        final CqlSession session = checkState(CqlSession.builder().withConfigLoader(configLoader).build());
        checkState(session);

        return session;
    }

    private static ProgrammaticDriverConfigLoaderBuilder newProgrammaticDriverConfigLoaderBuilder() {
        return DriverConfigLoader.programmaticBuilder()
            .withStringList(DefaultDriverOption.CONTACT_POINTS, CONTACT_POINTS)
            .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, USERNAME)
            .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, PASSWORD)
            .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, DefaultRetryPolicy.class);
    }

    private void testAllStatements(@NonNull final CqlSession session, @NonNull final String keyspaceName) {

        final String tableName = "sensor_data";

        try {

            assertThatCode(() ->
                createSchema(session, keyspaceName, tableName)
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
            this.cleanUp(session, keyspaceName);
        }
    }

    // endregion
}
