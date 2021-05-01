// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shows how to use the Cosmos extensions for DataStax Java Driver 4 for Apache Cassandra.
 * <p>
 * Best practices for configuring DataStax Java Driver 4 to access a Cosmos DB Cassandra API instance are also
 * demonstrated. See the settings in <a href="../../../../doc-files/application.conf.html">{@code application.conf}</a>
 * and <a href="../../../../doc-files/reference.conf.html">{@code reference.conf}</a>.
 * <p>
 * Ensure that your test runner does not set the module path when running this test. If you see a -p option on the
 * {@code java} command line, remote it. The test classes in this suite will not load the {@code referenc.conf} that
 * is included with azure-cosmos-cassandra-driver-4-extensions.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li> A Cosmos DB Cassandra API account is required. It should have at least two regions and may be configured for
 * multi-region writes or not. A number of system or--alternatively--environment variables must be set. See {@code
 * src/test/resources/application.conf} and {@link TestCommon} for a complete list. Their use and meaning should be
 * apparent from the relevant sections of the configuration and code.
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

    private static final Logger LOG = LoggerFactory.getLogger(CosmosCassandraIntegrationTest.class);

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
                "azure-cosmos-cassandra-driver-4-extensions").toString()));

    private static final int TIMEOUT_IN_SECONDS = 30;

    // endregion

    // region Methods

    /**
     * Verify that the extensions integrate with DataStax Java Driver 4 and its configuration system.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void canIntegrate(final boolean multiRegionWrites) {

        final CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(newConfigLoader(multiRegionWrites));

        try (final CqlSession session = builder.build()) {

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
    @BeforeAll
    public static void init(final TestInfo info) {

        LOG.info("---------------------------------------------------------------------------------------------------");
        LOG.info("{}", info.getTestClass().orElseGet(() -> fail("expected test to be called with test class")));
        LOG.info("---------------------------------------------------------------------------------------------------");

        if (REPORTING_DIRECTORY.exists()) {
            final Path path = REPORTING_DIRECTORY.toPath();
            assertThatCode(() -> Files.walk(path).map(Path::toFile).forEach(File::delete)).doesNotThrowAnyException();
        }

        assertThatCode(REPORTING_DIRECTORY::mkdirs).doesNotThrowAnyException();
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
                + ")"));;

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

    private static DriverConfigLoader newConfigLoader(final boolean multiRegionWrites) {
        return DriverConfigLoader.programmaticBuilder()
            .withBoolean(CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES, multiRegionWrites)
            .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, Collections.singletonList("cql-messages"))
            .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, Collections.singletonList("cql-requests"))
            .build();
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
