// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.Session;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testng.AssertJUnit.fail;

/**
 * This example illustrates use of the Cosmos extensions to Datastax Java Driver 4 for Apache Cassandra®.
 * <p>
 * Best practices for connecting to a Cosmos DB Cassandra API instance are also encapsulated in configuration. See the
 * settings in <a href="file:///../../../../../../resources/application.conf">{@code application.conf}</a> and
 * <a href="file:///../../../../../../../../package/src/main/resources/reference.conf">{@code reference.conf}</a>.
 * <h3>
 * Preconditions:
 * <ol>
 * <li>A Cosmos DB Cassandra API account is required.1
 * <li>These system variables or--alternatively--environment variables must be set.
 * <table>
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
 * Side effects:
 * <ol>
 * <li>Creates a keyspace in the cluster with replication factor 3. To prevent collisions especially during CI test
 * runs, we generate a keyspace name of the form <b>downgrading_</b><i></i><random-uuid></i>. Should a keyspace by this
 * name already exist, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * </li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.</ol>
 * <h3>
 * Notes:
 * <ul>
 * <li>You should never attempt to retry a non-idempotent write. See the driver's manual page on idempotence for more
 * information.</ul>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.9/manual/">DataStax Java Driver manual</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.9/manual/core/configuration/">DataStax Java Driver
 * configuration</a>
 * @see CosmosLoadBalancingPolicy
 * @see CosmosRetryPolicy
 * @see <a href="file:///../../../../../../resources/application.conf">application.conf</a>
 * @see <a href="file:///../../../../../../../../../package/src/main/resources/reference.conf">reference.conf</a>
 */
public class CosmosCassandraExtensionsExample implements AutoCloseable {

    // region Fields

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    private static final int TIMEOUT = 30_000;

    private CqlSession session;

    // endregion

    // region Methods

    @Test(groups = { "examples" }, timeOut = TIMEOUT)
    public void canIntegrateWithCosmos() {

        try (final Session ignored = this.connect()) {

            assertThatCode(this::createSchema).doesNotThrowAnyException();

            assertThatCode(() ->
                this.write(CONSISTENCY_LEVEL)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = this.read(CONSISTENCY_LEVEL);
                this.display(rows);
            }).doesNotThrowAnyException();

        } catch (final Exception error) {
            final StringWriter stringWriter = new StringWriter();
            error.printStackTrace(new PrintWriter(stringWriter));
            fail(format("connect failed with %s", stringWriter));
        }
    }

    /**
     * Closes the session and the cluster.
     */
    @Override
    public void close() {
        if (this.session != null) {
            this.session.close();
        }
    }

    // region Privates

    /**
     * Initiates a connection to the cluster specified by application.conf.
     */
    private Session connect() {
        this.session = CqlSession.builder().build();
        System.out.println("Connected to session: " + this.session.getName());
        return this.session;
    }

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    private void createSchema() {

        this.session.execute(SimpleStatement.newInstance(
            "CREATE KEYSPACE IF NOT EXISTS downgrading WITH replication = {"
                + "'class':'SimpleStrategy',"
                + "'replication_factor':3"
                + "}"));

        this.session.execute(SimpleStatement.newInstance(
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
    private ResultSet read(final ConsistencyLevel consistencyLevel) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        final SimpleStatement statement = SimpleStatement.newInstance(
            "SELECT sensor_id, date, timestamp, value "
                + "FROM downgrading.sensor_data "
                + "WHERE "
                + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
                + "date = '2018-02-26' AND "
                + "timestamp > '2018-02-26+01:00'")
            .setConsistencyLevel(consistencyLevel);

        final ResultSet rows = this.session.execute(statement);
        System.out.println("Read succeeded at " + consistencyLevel);

        return rows;
    }

    /**
     * Inserts data, retrying if necessary with a downgraded CL.
     *
     * @param consistencyLevel the consistency level to apply.
     */
    private void write(final ConsistencyLevel consistencyLevel) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED).setConsistencyLevel(consistencyLevel)
            .add(SimpleStatement.newInstance(
                "INSERT INTO downgrading.sensor_data "
                    + "(sensor_id, date, timestamp, value) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "'2018-02-26',"
                    + "'2018-02-26T13:53:46.345+01:00',"
                    + "2.34)"))
            .add(SimpleStatement.newInstance(
                "INSERT INTO downgrading.sensor_data "
                    + "(sensor_id, date, timestamp, value) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "'2018-02-26',"
                    + "'2018-02-26T13:54:27.488+01:00',"
                    + "2.47)"))
            .add(SimpleStatement.newInstance(
                "INSERT INTO downgrading.sensor_data "
                    + "(sensor_id, date, timestamp, value) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "'2018-02-26',"
                    + "'2018-02-26T13:56:33.739+01:00',"
                    + "2.52)"));

        this.session.execute(batch);
        System.out.println("Write succeeded at " + consistencyLevel);
    }

    // endregion
}
