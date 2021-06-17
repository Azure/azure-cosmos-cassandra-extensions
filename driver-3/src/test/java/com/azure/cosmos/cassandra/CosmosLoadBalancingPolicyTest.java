// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_HOSTNAME;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.cosmosClusterBuilder;
import static com.azure.cosmos.cassandra.TestCommon.testAllStatements;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static org.junit.jupiter.api.Assertions.fail;

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
 * during CI test runs, we generate keyspace names of the form <i>&lt;name&gt;</i><b><code>_</code></b><i>&lt;
 * random-uuid&gt;</i>. Should a keyspace by the generated name already exists, it is reused.
 * <li>Creates a table within each keyspace created or reused. If a table with a given name already exists, it is
 * reused.
 * <li>Executes all types of {@link Statement} queries.
 * </li>The keyspaces created or reused are then dropped. This prevents keyspaces from accumulating with repeated test
 * runs.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);
    private static final int TIMEOUT_IN_SECONDS = 3600;

    // endregion

    // region Methods

    @BeforeAll
    static void touchCosmosLoadBalancingPolicy() {
        CosmosLoadBalancingPolicy.builder(); // forces class initialization
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} without preferred regions issues routes requests correctly.
     * <p>
     * All requests should go to the global endpoint with failover to endpoints in other regions in alphabetic order. We
     * do not test the failover scenario here. We simply check that all requests are sent to the global endpoint whether
     * or not multi-region writes are enabled. Cosmos DB guarantees that both read and write requests can be sent to the
     * global endpoint.
     *
     * @param multiRegionWrites {@code true}, if the test should be run with multi-region writes enabled.
     */
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithoutPreferredRegions(final boolean multiRegionWrites) {

        final Cluster.Builder builder = cosmosClusterBuilder(
            CosmosLoadBalancingPolicy.builder()
                .withMultiRegionWrites(multiRegionWrites)
                .withPreferredRegions(Collections.emptyList())
                .build())
            .withClusterName("testWithoutPreferredRegions.multiRegionWrites=" + multiRegionWrites)
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD);

        try (Cluster cluster = builder.build()) {
            testAllStatements(cluster.connect());
        } catch (final AssertionError error) {
            throw error;
        } catch (final Throwable error) {
            fail("Failed due to: " + toJson(error), error);
        }
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} with preferred regions issues routes requests correctly.
     * <p>
     * The behavior varies based on whether multi-region writes are enabled. When multi-region writes are enabled all
     * requests (reads and writes) will be sent to the first region in the list of preferred regions with failover to
     * other regions in the preferred region list in the order in which they are listed. If all regions in the preferred
     * region list are down, all requests will fall back to the region in which the global endpoint is deployed, then to
     * other regions in alphabetic order.
     * <p>
     * When multi-region writes are disabled, all write requests are sent to the global-endpoint. There is no fallback.
     * Read requests are handled the same whether or not multi-region writes are enabled.
     * We do not test failover scenarios here. We simply check that all requests are sent to the first preferred region
     * when multi-region writes are enabled. When multi-region writes are disabled we verify that all read requests are
     * sent to the first preferred region and that all write requests are sent to the global endpoint address.
     *
     * @param multiRegionWrites {@code true}, if the test should be run with multi-region writes enabled.
     */
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithPreferredRegions(final boolean multiRegionWrites) {

        final Cluster.Builder builder = cosmosClusterBuilder(
                CosmosLoadBalancingPolicy.builder()
                    .withMultiRegionWrites(multiRegionWrites)
                    .withPreferredRegions(PREFERRED_REGIONS)
                    .build(),
                CosmosRetryPolicy.defaultPolicy())
            .withClusterName("testWithPreferredRegions.multiRegionWrites=" + multiRegionWrites)
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD);

        try (Cluster cluster = builder.build()) {
            testAllStatements(cluster.connect());
        } catch (final AssertionError error) {
            throw error;
        } catch (final Throwable error) {
            fail("Failed due to: " + toJson(error), error);
        }
    }

    // endregion

    // region Privates

    // endregion
}
