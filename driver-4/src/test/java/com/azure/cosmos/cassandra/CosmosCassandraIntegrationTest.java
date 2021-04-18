// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Shows how to use the Cosmos extensions for DataStax Java Driver 4 for Apache Cassandra.
 * <p>
 * Best practices for configuring DataStax Java Driver 4 to access a Cosmos DB Cassandra API instance are also
 * demonstrated. See the settings in <a href="../../../../doc-files/application.conf.html">{@code application.conf}</a>
 * and <a href="../../../../doc-files/reference.conf.html">{@code reference.conf}</a>.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li>A Cosmos DB Cassandra API account is required.
 * <li>These system variables or--alternatively--environment variables must be set.
 * <table><caption></caption>
 * <thead>
 * <tr>
 * <th>System variable</th>
 * <th>Environment variable</th>
 * <th>Description</th></tr>
 * </thead>
 * <tbody>
 * <tr>
 * <td>datastax-java-driver.basic.load-balancing-policy.global-endpoint</td>
 * <td>AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT</td>
 * <td>Global endpoint address (e.g., "database-account.cassandra.cosmos.azure.com:10350")</td></tr>
 * <tr>
 * <td>azure.cosmos.cassandra.read-datacenter</td>
 * <td>AZURE_COSMOS_CASSANDRA_READ_DATACENTER</td>
 * <td>Read datacenter name (e.g., "East US")</td></tr>
 * <tr>
 * <td>azure.cosmos.cassandra.write-datacenter</td>
 * <td>AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER</td>
 * <td>Write datacenter name (e.g., "West US")</td></tr>
 * <tr>
 * <td>datastax-java-driver.advanced.auth-provider.username</td>
 * <td>AZURE_COSMOS_CASSANDRA_USERNAME</td>
 * <td>Username for authentication</td></tr>
 * <tr>
 * <td>datastax-java-driver.advanced.auth-provider.password</td>
 * <td>AZURE_COSMOS_CASSANDRA_PASSWORD</td>
 * <td>Password for authentication</td></tr>
 * </tbody>
 * </table>
 * </ol>
 * <h3>
 * Side effects</h3>
 * <ol>
 * <li>Creates a keyspace in the cluster with replication factor 4, the number of replicas per partition in a Cosmos DB
 * instance. To prevent collisions especially during CI test runs, we generate a keyspace name of the form 
 * <b>downgrading_</b><i>&gt;random-uuid&lt;</i>. Should a keyspace by this name already exist, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * <li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.</ol>
 * <h3>
 * Notes</h3>
 * <ul>
 * <ul>
 * <li>You should never attempt to retry a non-idempotent write. See the driver's manual page on idempotence for more
 * information.</ul>
 *
 * @see CosmosLoadBalancingPolicy
 * @see CosmosRetryPolicy
 * @see <a href="../../../../doc-files/application.conf.html">application.conf</a>
 * @see <a href="../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/">DataStax Java Driver manual</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/">DataStax Java
 * Driver configuration</a>
 */

public class CosmosCassandraIntegrationTest {

    // region Fields

    private static final ConsistencyLevel CONSISTENCY_LEVEL = Enum.valueOf(DefaultConsistencyLevel.class,
        TestCommon.getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.consistency-level",
            "AZURE_COSMOS_CASSANDRA_CONSISTENCY_LEVEL",
            "QUORUM"));

    private static final String KEYSPACE_NAME = TestCommon.getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.keyspace-name",
        "AZURE_COSMOS_CASSANDRA_KEYSPACE_NAME",
        "downgrading_" + UUID.randomUUID().toString().replace("-", ""));

    private static final File REPORTING_DIRECTORY = new File(
        TestCommon.getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.reporting-directory",
            "AZURE_COSMOS_CASSANDRA_REPORTING_DIRECTORY",
            Paths.get(
                System.getProperty("user.home"),
                ".local",
                "var",
                "lib",
                "azure-cosmos-cassandra-driver-4").toString()));

    private static final int TIMEOUT_IN_MILLIS = 30_000;

    // endregion

    // region Methods

    /**
     * Verify that the extensions integrate with DataStax Java Driver 4 and its configuration system.
     *
     * @param loader A {@linkplain DriverConfigLoader loader} of the configuration under test.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Test(groups = { "integration" }, dataProvider = "test-cases", timeOut = TIMEOUT_IN_MILLIS)
    public void canIntegrate(@NonNull final DriverConfigLoader loader) {

        try (final CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {

            //noinspection SimplifyOptionalCallChains
            if (!session.getMetrics().isPresent()) {
                throw new NoSuchElementException("session metrics are unavailable");
            }

            final Metrics metrics = session.getMetrics().get();

            //noinspection SimplifyOptionalCallChains
            if (!metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS).isPresent()) {
                throw new NoSuchElementException(DefaultSessionMetric.CQL_REQUESTS + " metrics are unavailable");
            }

            final Timer sessionRequestTimer = (Timer) metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS).get();
            final AtomicLong expectedRowCount = new AtomicLong();
            long expectedRequestCount = 0;

            try (final ScheduledReporter reporter = CsvReporter.forRegistry(metrics.getRegistry())
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .build(REPORTING_DIRECTORY)) {

                assertThatCode(() -> this.createSchema(session)).doesNotThrowAnyException();
                assertThat(sessionRequestTimer.getCount()).isEqualTo(expectedRequestCount += 3);

                assertThatCode(() -> expectedRowCount.set(this.write(session))).doesNotThrowAnyException();
                assertThat(sessionRequestTimer.getCount()).isEqualTo(expectedRequestCount += 1);

                assertThatCode(() -> {
                    final List<Row> rows = this.read(session).all();
                    this.display(rows);
                    assertThat(rows.size()).isEqualTo(expectedRowCount.get());
                }).doesNotThrowAnyException();

                assertThat(sessionRequestTimer.getCount()).isEqualTo(expectedRequestCount + 1);
                reporter.report();

            } finally {
                assertThatCode(() ->
                    session.execute(SimpleStatement.newInstance("DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME))
                ).doesNotThrowAnyException();
            }

        } catch (final AssertionError error) {

            // Reason: verification failure
            throw error;

        } catch (final AllNodesFailedException error) {

            // Reason: connection failure

            final StringWriter message = new StringWriter();
            final PrintWriter writer = new PrintWriter(message);

            writer.printf("Connection failure: %s. The complete list of errors grouped by node follows:%n",
                error.getMessage().replaceFirst(
                    " \\(showing first \\d+ nodes, use getAllErrors.+",
                    ""));

            for (final Map.Entry<Node, List<Throwable>> entry : error.getAllErrors().entrySet()) {
                writer.printf("%n%s%n", entry.getKey());
                for (final Throwable cause : entry.getValue()) {
                    cause.printStackTrace(writer);
                }
            }

            throw new AssertionError(message);

        } catch (final Throwable error) {

            // Reason: unexpected failure
            final StringWriter stringWriter = new StringWriter();
            error.printStackTrace(new PrintWriter(stringWriter));
            throw new AssertionError(format("unexpected failure:%n%s", stringWriter));
        }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeClass
    public void init() {
        REPORTING_DIRECTORY.mkdirs();
    }

    /** Provides the list of {@link CosmosCassandraIntegrationTest} cases.
     *
     * @return The list of {@link CosmosCassandraIntegrationTest} cases.
     */
    @DataProvider(name = "test-cases")
    public static Object[][] testCases() {

        final int count = TestCommon.READ_DATACENTER.isEmpty() ? 1 : (TestCommon.WRITE_DATACENTER.isEmpty() ? 2 : 3);
        final Object[][] testCases = new Object[count][3];

        testCases[0] = new Object[] { newDriverConfigLoader(TestCommon.GLOBAL_ENDPOINT, "", "") };

        if (TestCommon.READ_DATACENTER.isEmpty()) {
            return testCases;
        }

        testCases[1] = new Object[] { newDriverConfigLoader(TestCommon.GLOBAL_ENDPOINT, TestCommon.READ_DATACENTER, "") };

        if (TestCommon.WRITE_DATACENTER.isEmpty()) {
            return testCases;
        }

        testCases[2] = new Object[] { newDriverConfigLoader("", TestCommon.READ_DATACENTER, TestCommon.WRITE_DATACENTER) };
        return testCases;
    }

    // region Privates

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    private void createSchema(final CqlSession session) throws InterruptedException {

        session.execute(SimpleStatement.newInstance(
            "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication = {"
                + "'class':'SimpleStrategy',"
                + "'replication_factor':4"
                + "}"));

        session.execute(SimpleStatement.newInstance("DROP TABLE IF EXISTS " + KEYSPACE_NAME + ".sensor_date"));

        session.execute(SimpleStatement.newInstance(
            "CREATE TABLE " + KEYSPACE_NAME + ".sensor_data ("
                + "sensor_id uuid,"
                + "date date,"
                + "timestamp timestamp,"
                + "value double,"
                + "PRIMARY KEY ((sensor_id,date),timestamp)"
                + ")"));

        Thread.sleep(5_000L);  // gives time for the table creation to sync across regions
    }

    /**
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    private void display(final List<Row> rows) {

        final int width1 = 38;
        final int width2 = 12;
        final int width3 = 30;
        final int width4 = 21;

        final String format = "%-" + width1 + "s" + "%-" + width2 + "s" + "%-" + width3 + "s" + "%-" + width4 + "s%n";

        System.out.println();
        System.out.printf(format, "sensor_id", "date", "timestamp", "value");

        drawLine(width1, width2, width3, width4);

        for (final Row row : rows) {
            System.out.printf(format,
                row.getUuid("sensor_id"),
                row.getLocalDate("date"),
                row.getInstant("timestamp"),
                row.getDouble("value"));
        }

        System.out.println();
    }

    /**
     * Draws a line to isolate headings from rows.
     *
     * @param widths the column widths.
     */
    private static void drawLine(final int... widths) {
        for (final int width : widths) {
            for (int i = 1; i < width; i++) {
                System.out.print('-');
            }
            System.out.print('+');
        }
        System.out.println();
    }

    private static String getFullyQualifiedPath(final CosmosLoadBalancingPolicyOption option) {
        return "datastax-java-driver." + option.getPath();
    }

    private static DriverConfigLoader newDriverConfigLoader(
        final String globalEndpoint,
        final String readDatacenter,
        final String writeDatacenter) {

        System.setProperty(getFullyQualifiedPath(CosmosLoadBalancingPolicyOption.GLOBAL_ENDPOINT), globalEndpoint);
        System.setProperty(getFullyQualifiedPath(CosmosLoadBalancingPolicyOption.READ_DATACENTER), readDatacenter);
        System.setProperty(getFullyQualifiedPath(CosmosLoadBalancingPolicyOption.WRITE_DATACENTER), writeDatacenter);

        return DriverConfigLoader.fromClasspath("cosmos-cassandra-integration-test.conf");
    }

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     */
    private ResultSet read(final CqlSession session) {

        System.out.printf("Read from %s.sensor_data at %s ... ", KEYSPACE_NAME, CONSISTENCY_LEVEL);

        final SimpleStatement statement = SimpleStatement.newInstance(
            "SELECT sensor_id, date, timestamp, value "
                + "FROM " + KEYSPACE_NAME + ".sensor_data "
                + "WHERE "
                + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                + "date = '2018-02-26' AND "
                + "timestamp > '2018-02-26+01:00'")
            .setConsistencyLevel(CONSISTENCY_LEVEL);

        final ResultSet rows = session.execute(statement);
        System.out.printf("succeeded%n");

        return rows;
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     */
    private long write(final CqlSession session) {

        System.out.printf("Write to %s.sensor_data at %s ... ", KEYSPACE_NAME, CONSISTENCY_LEVEL);

        final BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED)
            .add(SimpleStatement.newInstance("INSERT INTO " + KEYSPACE_NAME + ".sensor_data "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)"))
            .add(SimpleStatement.newInstance("INSERT INTO " + KEYSPACE_NAME + ".sensor_data "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:54:27.488+01:00',"
                + "2.47)"))
            .add(SimpleStatement.newInstance("INSERT INTO " + KEYSPACE_NAME + ".sensor_data "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:56:33.739+01:00',"
                + "2.52)"))
            .setConsistencyLevel(CONSISTENCY_LEVEL);

        session.execute(batch);

        System.out.printf("succeeded%n");
        return batch.size();
    }

    // endregion
}
