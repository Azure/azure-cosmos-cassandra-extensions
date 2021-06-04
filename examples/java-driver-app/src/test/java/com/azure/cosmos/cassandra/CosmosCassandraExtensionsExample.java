// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.UUID;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

/**
 * Illustrates use of the Cosmos Extensions for DataStax Java Driver 3 for Apache Cassandra®.
 * <p>
 * Best practices for configuring DataStax Java Driver 3 to access a Cosmos DB Cassandra API instance are also
 * demonstrated. See {@link #connect}.
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
 * <td>azure.cosmos.cassandra.global-endpoint</td>
 * <td>AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT</td>
 * <td>Global endpoint address (e.g., "database-account.cassandra.cosmos.azure.com:10350")</td></tr>
 * <tr>
 * <td>azure.cosmos.cassandra.read-datacenter</td>
 * <td>AZURE_COSMOS_CASSANDRA_READ_DATACENTER</td>
 * <td>Read datacenter name. Example: {@code "East US"}</td></tr>
 * <tr>
 * <td>azure.cosmos.cassandra.write-datacenter</td>
 * <td>AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER</td>
 * <td>Write datacenter name. Example: {@code "West US"}.</td></tr>
 * <tr>
 * <td>datastax-java-driver.advanced.auth-provider.username</td>
 * <td>AZURE_COSMOS_CASSANDRA_USERNAME</td>
 * <td>Username for authentication.</td></tr>
 * <tr>
 * <td>datastax-java-driver.advanced.auth-provider.password</td>
 * <td>AZURE_COSMOS_CASSANDRA_PASSWORD</td>
 * <td>Password for authentication.</td></tr>
 * </tbody>
 * </table>
 * </ol>
 * <h3>
 * Side effects</h3>
 * <ol>
 * <li>Creates a keyspace in the cluster with replication factor 3. To prevent collisions especially during CI test
 * runs, we generate a keyspace names of the form <b>downgrading_</b><i>&lt;random-uuid&gt;</i>. Should a keyspace by
 * this name already exists, it is reused.
 * <li>Creates a table within the keyspace created or reused. If a table with the given name already exists, it is
 * reused.
 * <li>The keyspace created or reused is then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.</ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosCassandraExtensionsExample {

    // region Fields

    static final String GLOBAL_ENDPOINT = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.global-endpoint",
            "AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT",
            "localhost:10350");

    static final String GLOBAL_ENDPOINT_HOSTNAME;
    static final int GLOBAL_ENDPOINT_PORT;

    static final String PASSWORD = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.password",
            "AZURE_COSMOS_CASSANDRA_PASSWORD",
            "");

    static final String READ_DATACENTER = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.read-datacenter",
            "AZURE_COSMOS_CASSANDRA_READ_DATACENTER",
            "");

    static final String USERNAME = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.username",
            "AZURE_COSMOS_CASSANDRA_USERNAME",
            "");

    static final String WRITE_DATACENTER = getPropertyOrEnvironmentVariable(
            "azure.cosmos.cassandra.write-datacenter",
            "AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER",
            "");

    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final String KEYSPACE_NAME = "downgrading_" + UUID.randomUUID().toString().replace("-", "");
    private static final int MAX_RETRY_COUNT = 5;
    private static final int TIMEOUT_IN_MILLISECONDS = 30_000;

    static {

        final int index = GLOBAL_ENDPOINT.lastIndexOf(':');
        assertThat(index).isGreaterThan(0);

        final String hostname = GLOBAL_ENDPOINT.substring(0, index);
        final String port = GLOBAL_ENDPOINT.substring(index + 1);
        GLOBAL_ENDPOINT_HOSTNAME = hostname;

        int value = -1;

        try {
            value = Integer.parseUnsignedInt(port);
            assertThat(value).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(65535);
        } catch (final Throwable error) {
            fail("expected integer port number in range [0, 65535], not " + port);
        }

        GLOBAL_ENDPOINT_PORT = value;
    }

    // endregion

    // region Methods

    /**
     * Shows how to integrate with a Cosmos Cassandra API instance using azure-cosmos-cassandra-extensions.
     */
    @Test(groups = { "examples" }, timeOut = TIMEOUT_IN_MILLISECONDS)
    public void canIntegrateWithCosmos() {

        final Session session = connect();

        try {
            assertThatCode(() -> this.createSchema(session)).doesNotThrowAnyException();

            try {
                Thread.sleep(5_000);
            } catch (final InterruptedException error) {
                System.out.println("sleep interrupted");
            }

            assertThatCode(() ->
                    this.write(session, CONSISTENCY_LEVEL)
            ).doesNotThrowAnyException();

            assertThatCode(() -> {
                final ResultSet rows = this.read(session, CONSISTENCY_LEVEL);
                this.display(rows);
            }).doesNotThrowAnyException();

        } finally {
            cleanUp(session);
        }
    }

    // endregion

    // Privates

    /**
     * Drops {@link #KEYSPACE_NAME} and closes the {@link Session session} and the {@linkplain Cluster cluster} it
     * references.
     */
    private static void cleanUp(final Session session) {
        if (session != null && !session.isClosed()) {
            try {
                session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME);
            } finally {
                session.close();
                session.getCluster().close();
            }
        }
    }

    /**
     * Initiates a connection to the cluster specified by the given contact points and port.
     * <p>
     * Default options that are relevant to performance and reliability when accessing a Cosmos DB Cassandra API
     * instance are called out explicitly here. Recommendation: Use the defaults for the options that are specified
     * here.
     */
    private static Session connect() {

        final Cluster cluster = Cluster.builder()
            .withLoadBalancingPolicy(CosmosLoadBalancingPolicy.builder()
                .withGlobalEndpoint(WRITE_DATACENTER.isEmpty() ? GLOBAL_ENDPOINT : "")
                .withReadDC(READ_DATACENTER)
                .withWriteDC(WRITE_DATACENTER)
                .build())
            // Cosmos DB load-balances requests against a large number of backend nodes. Experiments show that these
            // values for local and remote node sizes work well in development, test, and low-volume production or
            // staging environments. You should increase these values based on the RUs provisioned for your database.
            //
            //   RUs      | local.size | remote.size
            //   ---------+------------+-------------
            //   100,000  | 50-100     | 50-100
            //   200,000+ | 100        | 100
            .withPoolingOptions(new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 10, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 1, 10))
            .withReconnectionPolicy(new ConstantReconnectionPolicy(1_000))
            .withRetryPolicy(CosmosRetryPolicy.builder().build())
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoints(GLOBAL_ENDPOINT_HOSTNAME)
            .withPort(GLOBAL_ENDPOINT_PORT)
            .withSSL()
            .build();

        try {
            return cluster.connect();
        } catch (final Throwable error) {
            cluster.close();
            throw error;
        }
    }

    /**
     * Creates the schema (keyspace) and table to verify that we can integrate with Cosmos.
     */
    private void createSchema(final Session session) {

        session.execute("CREATE KEYSPACE IF NOT EXISTS "
                + KEYSPACE_NAME
                + " WITH replication "
                + "= {'class': 'SimpleStrategy', 'replication_factor': 3}");

        session.execute("CREATE TABLE IF NOT EXISTS "
                + KEYSPACE_NAME
                + ".sensor_data ("
                + "sensor_id uuid,"
                + "date date,"
                + // emulates bucketing by day
                "timestamp timestamp,"
                + "value double,"
                + "PRIMARY KEY ((sensor_id,date),timestamp)"
                + ")");
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

        final String format = "%-" + width1 + "s" + "%-" + width2 + "s" + "%-" + width3 + "s" + "%-" + width4 + "s"
                + "%n";

        System.out.printf(format, "sensor_id", "date", "timestamp", "value");
        drawLine(width1, width2, width3, width4);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        for (final Row row : rows) {
            System.out.printf(
                    format,
                    row.getUUID("sensor_id"),
                    row.getDate("date"),
                    sdf.format(row.getTimestamp("timestamp")),
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

    private static String getPropertyOrEnvironmentVariable(
            final String property, final String variable, final String defaultValue) {

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
     * Queries data, retrying if necessary with a downgraded CL.
     *
     * @param session          the session for executing the operation.
     * @param consistencyLevel the consistency level to apply.
     */
    private ResultSet read(final Session session, final ConsistencyLevel consistencyLevel) {

        System.out.printf("Reading at %s%n", consistencyLevel);

        final Statement statement = new SimpleStatement(
                "SELECT sensor_id, date, timestamp, value "
                        + "FROM "
                        + KEYSPACE_NAME
                        + ".sensor_data "
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
    private void write(final Session session, final ConsistencyLevel consistencyLevel) {

        System.out.printf("Writing at %s%n", consistencyLevel);

        final BatchStatement batch = new BatchStatement(UNLOGGED)
                .add(new SimpleStatement("INSERT INTO "
                        + KEYSPACE_NAME
                        + ".sensor_data "
                        + "(sensor_id, date, timestamp, value) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'2018-02-26',"
                        + "'2018-02-26T13:53:46.345+01:00',"
                        + "2.34)"))
                .add(new SimpleStatement("INSERT INTO "
                        + KEYSPACE_NAME
                        + ".sensor_data "
                        + "(sensor_id, date, timestamp, value) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'2018-02-26',"
                        + "'2018-02-26T13:54:27.488+01:00',"
                        + "2.47)"))
                .add(new SimpleStatement("INSERT INTO "
                        + KEYSPACE_NAME
                        + ".sensor_data "
                        + "(sensor_id, date, timestamp, value) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'2018-02-26',"
                        + "'2018-02-26T13:56:33.739+01:00',"
                        + "2.52)"));

        batch.setConsistencyLevel(consistencyLevel);
        session.execute(batch);

        System.out.println("Write succeeded at " + consistencyLevel);
    }

    // endregion
}
