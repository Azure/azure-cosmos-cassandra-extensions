// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testng.AssertJUnit.fail;

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
 * <li>Creates a keyspace in the cluster with replication factor 3. To prevent collisions especially during CI test
 * runs, we generate a keyspace name of the form <b>downgrading_</b><i>&gt;random-uuid&lt;</i>. Should a keyspace by
 * this name already exist, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * <li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.</ol>
 * <h3>
 * Notes</h3>
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
public class CosmosCassandraExtensionsExample {

    // region Fields

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private static final File REPORTING_DIRECTORY = new File(getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.reporting-directory",
        "AZURE_COSMOS_CASSANDRA_REPORTING_DIRECTORY",
        System.getProperty("user.home")));

    private static final int TIMEOUT_IN_MILLIS = 30_000;

    // endregion

    // region Methods

    /**
     * Shows how to integrate with a Cosmos Cassandra API instance using azure-cosmos-cassandra-driver-4-extensions.
     */
    @Test(groups = { "examples" }, timeOut = TIMEOUT_IN_MILLIS)
    public void canIntegrateWithCosmos() {

        try (final CqlSession session = CqlSession.builder().build()) {

            if (session.getMetrics().isEmpty()) {
                throw new NoSuchElementException("session metrics are unavailable");
            }

            final Metrics metrics = session.getMetrics().get();

            if (metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS).isEmpty()) {
                throw new NoSuchElementException(DefaultSessionMetric.CQL_REQUESTS + " metrics are unavailable");
            }

            final Timer sessionRequestTimer = (Timer) metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS).get();
            long requestCount = 0;

            try (final ScheduledReporter reporter = CsvReporter.forRegistry(metrics.getRegistry())
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .build(REPORTING_DIRECTORY)) {

                assertThat(sessionRequestTimer.getCount()).isEqualTo(requestCount);

                assertThatCode(() -> this.createSchema(session)).doesNotThrowAnyException();
                assertThat(sessionRequestTimer.getCount()).isEqualTo(requestCount += 2);

                assertThatCode(() -> this.write(session, CONSISTENCY_LEVEL)).doesNotThrowAnyException();
                assertThat(sessionRequestTimer.getCount()).isEqualTo(requestCount += 1);

                assertThatCode(() -> {
                    final ResultSet rows = this.read(session, CONSISTENCY_LEVEL);
                    this.display(rows);
                }).doesNotThrowAnyException();

                assertThat(sessionRequestTimer.getCount()).isEqualTo(requestCount + 1);
                reporter.report();
            }

        } catch (final Throwable error) {
            final StringWriter stringWriter = new StringWriter();
            error.printStackTrace(new PrintWriter(stringWriter));
            fail(format("failed with %s", stringWriter));
        }
    }

    // region Privates

    /**
     * Get the value of the specified system {@code property} or--if it is unset--environment {@code variable}.
     * <p>
     * If neither {@code property} or {@code variable} is set, {@code defaultValue} is returned.
     *
     * @param property     a system property name.
     * @param variable     an environment variable name.
     * @param defaultValue the default value--which may be {@code null}--to be used if neither {@code property} or
     *                     {@code variable} is set.
     *
     * @return The value of the specified {@code property}, the value of the specified environment {@code variable}, or
     * {@code defaultValue}.
     */
    @SuppressWarnings("SameParameterValue")
    static String getPropertyOrEnvironmentVariable(
        @NonNull final String property, @NonNull final String variable, final String defaultValue) {

        String value = System.getProperty(property);

        if (value == null) {
            value = System.getenv(variable);
        }

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    private void createSchema(final CqlSession session) {

        session.execute(SimpleStatement.newInstance(
            "CREATE KEYSPACE IF NOT EXISTS downgrading WITH replication = {"
                + "'class':'SimpleStrategy',"
                + "'replication_factor':3"
                + "}"));

        session.execute(SimpleStatement.newInstance(
            "CREATE TABLE IF NOT EXISTS downgrading.sensor_data ("
                + "sensor_id uuid,"
                + "date date,"
                + "timestamp timestamp,"
                + "value double,"
                + "PRIMARY KEY ((sensor_id,date),timestamp)"
                + ")"));
    }

    /**
     * Displays the results on the console.
     *
     * @param rows the results to display.
     */
    private void display(final ResultSet rows) {

        final int width1 = 38;
        final int width2 = 12;
        final int width3 = 30;
        final int width4 = 21;

        final String format = "%-" + width1 + "s" + "%-" + width2 + "s" + "%-" + width3 + "s" + "%-" + width4 + "s%n";
        System.out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        for (final Row row : rows) {
            System.out.printf(format,
                row.getUuid("sensor_id"),
                row.getLocalDate("date"),
                row.getInstant("timestamp"),
                row.getDouble("value"));
        }
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

    /**
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private ResultSet read(final CqlSession session, final ConsistencyLevel consistencyLevel) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        final SimpleStatement statement = SimpleStatement.newInstance(
            "SELECT sensor_id, date, timestamp, value "
                + "FROM downgrading.sensor_data "
                + "WHERE "
                + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                + "date = '2018-02-26' AND "
                + "timestamp > '2018-02-26+01:00'")
            .setConsistencyLevel(consistencyLevel);

        final ResultSet rows = session.execute(statement);
        System.out.println("Read succeeded at " + consistencyLevel);

        return rows;
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private void write(final CqlSession session, final ConsistencyLevel consistencyLevel) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED)
            .add(SimpleStatement.newInstance("INSERT INTO downgrading.sensor_data "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)"))
            .add(SimpleStatement.newInstance("INSERT INTO downgrading.sensor_data "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:54:27.488+01:00',"
                + "2.47)"))
            .add(SimpleStatement.newInstance("INSERT INTO downgrading.sensor_data "
                + "(sensor_id, date, timestamp, value) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26',"
                + "'2018-02-26T13:56:33.739+01:00',"
                + "2.52)"))
            .setConsistencyLevel(consistencyLevel);

        session.execute(batch);
        System.out.println("Write succeeded at " + consistencyLevel);
    }

    // endregion
}
