// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_HOSTNAME;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_PORT;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.REGIONAL_ENDPOINTS;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.cosmosClusterBuilder;
import static com.azure.cosmos.cassandra.TestCommon.testAllStatements;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test verifies that the {@link CosmosLoadBalancingPolicy} class routes requests correctly.
 * <h3>
 * Preconditions</h3>
 * <p>
 * A Cosmos Cassandra API account is required. It should have at least two regions with multi-region writes
 * enabled.
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);
    private static final int TIMEOUT_IN_SECONDS = 3600;

    // endregion

    // region Methods

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} with preferred regions routes requests correctly.
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
     * <p>
     * This test targets multi-region Cosmos Cassandra API instances. It is guaranteed to fail when run against
     * multi-node Apache Cassandra datacenters.
     *
     * @param multiRegionWritesEnabled {@code true}, if the test should be run with multi-region writes enabled.
     */
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithPreferredRegions(final boolean multiRegionWritesEnabled) {

        final Cluster.Builder builder = cosmosClusterBuilder(
                CosmosLoadBalancingPolicy.builder()
                    .withMultiRegionWrites(multiRegionWritesEnabled)
                    .withPreferredRegions(PREFERRED_REGIONS)
                    .build(),
                CosmosRetryPolicy.defaultPolicy())
            .withClusterName("testWithPreferredRegions.multiRegionWritesEnabled=" + multiRegionWritesEnabled)
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD);

        try (Cluster cluster = builder.build()) {

            final CosmosLoadBalancingPolicy policy = (CosmosLoadBalancingPolicy) cluster.getConfiguration()
                .getPolicies()
                .getLoadBalancingPolicy();

            final Metadata clusterMetadata = cluster.getMetadata();

            final InetSocketAddress globalEndpointAddress = new InetSocketAddress(
                GLOBAL_ENDPOINT_HOSTNAME,
                GLOBAL_ENDPOINT_PORT);

            // This is the expected state of the instance immediately after a cluster is built, before the first session
            // is created. Starting out we've sometimes got just one host: the global host with a control channel and a
            // newly-created request pool. Our checks in this phase do not require that all regional (failover) hosts
            // have been enumerated.

            final Set<Host> initialHosts = clusterMetadata.getAllHosts();

            final Host globalHost = initialHosts.stream()
                .filter(host -> host.getBroadcastRpcAddress().equals(globalEndpointAddress))
                .toArray(Host[]::new)[0];

            final List<String> expectedPreferredRegions = new ArrayList<>(PREFERRED_REGIONS);

            if (!PREFERRED_REGIONS.contains(globalHost.getDatacenter())) {
                expectedPreferredRegions.add(globalHost.getDatacenter());
            }

            final Host[] initialExpectedPreferredHosts = initialHosts.stream()
                .filter(host -> expectedPreferredRegions.contains(host.getDatacenter()))
                .sorted(Comparator.comparingInt(x -> expectedPreferredRegions.indexOf(x.getDatacenter())))
                .toArray(Host[]::new);

            final Host[] initialExpectedFailoverHosts = initialHosts.stream()
                .filter(host -> !expectedPreferredRegions.contains(host.getDatacenter()))
                .sorted(Comparator.comparing(Host::getDatacenter))
                .toArray(Host[]::new);

            validateOperationalState(
                policy,
                globalHost,
                multiRegionWritesEnabled,
                expectedPreferredRegions,
                initialExpectedPreferredHosts,
                initialExpectedFailoverHosts);

            try (Session session = cluster.connect()) {

                Set<Host> hosts = Collections.emptySet();

                for (int i = 0; i < 10; i++) {

                    testAllStatements(session);
                    hosts = clusterMetadata.getAllHosts();

                    if (hosts.size() == REGIONAL_ENDPOINTS.size()) {
                        break;
                    }
                }

                assertThat(hosts).hasSize(REGIONAL_ENDPOINTS.size());

                final Host[] expectedPreferredHosts = hosts.stream()
                    .filter(host -> expectedPreferredRegions.contains(host.getDatacenter()))
                    .sorted(Comparator.comparingInt(x -> expectedPreferredRegions.indexOf(x.getDatacenter())))
                    .toArray(Host[]::new);

                assertThat(expectedPreferredHosts).hasSize(expectedPreferredRegions.size());

                final Host[] expectedFailoverHosts = hosts.stream()
                    .filter(host -> !expectedPreferredRegions.contains(host.getDatacenter()))
                    .sorted(Comparator.comparing(Host::getDatacenter))
                    .toArray(Host[]::new);

                validateOperationalState(
                    policy,
                    globalHost,
                    multiRegionWritesEnabled,
                    expectedPreferredRegions,
                    expectedPreferredHosts,
                    expectedFailoverHosts);
            }

        } catch (final AssertionError error) {
            throw error;
        } catch (final Throwable error) {
            fail("Failed due to: " + toJson(error), error);
        }
    }

    /**
     * Verifies that a {@link CosmosLoadBalancingPolicy} without preferred regions routes requests correctly.
     * <p>
     * All requests should go to the global endpoint with failover to endpoints in other regions in alphabetic order. We
     * do not test the failover scenario here. We simply check that all requests are sent to the global endpoint whether
     * or not multi-region writes are enabled. Cosmos DB guarantees that both read and write requests can be sent to the
     * global endpoint.
     * <p>
     * This test targets multi-region Cosmos Cassandra API instances. It is guaranteed to fail when run against
     * multi-node Apache Cassandra datacenters.
     *
     * @param multiRegionWritesEnabled {@code true}, if the test should be run with multi-region writes enabled.
     */
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithoutPreferredRegions(final boolean multiRegionWritesEnabled) {

        final Cluster.Builder builder = cosmosClusterBuilder(
            CosmosLoadBalancingPolicy.builder()
                .withMultiRegionWrites(multiRegionWritesEnabled)
                .withPreferredRegions(Collections.emptyList())
                .build())
            .withClusterName("testWithoutPreferredRegions.multiRegionWritesEnabled=" + multiRegionWritesEnabled)
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD);

        try (Cluster cluster = builder.build()) {

            final CosmosLoadBalancingPolicy policy = (CosmosLoadBalancingPolicy) cluster.getConfiguration()
                .getPolicies()
                .getLoadBalancingPolicy();

            final Metadata clusterMetadata = cluster.getMetadata();

            final InetSocketAddress globalEndpointAddress = new InetSocketAddress(
                GLOBAL_ENDPOINT_HOSTNAME,
                GLOBAL_ENDPOINT_PORT);

            // This is the expected state of the instance immediately after a cluster is built, before the first session
            // is created. Starting out we've sometimes got just one host: the global host with a control channel and a
            // newly-created request pool. Our checks in this phase do not require that all regional (failover) hosts
            // have been enumerated.

            final Set<Host> initialHosts = clusterMetadata.getAllHosts();

            final Host[] expectedPreferredHosts = initialHosts.stream()
                .filter(host -> host.getBroadcastRpcAddress().equals(globalEndpointAddress))
                .toArray(Host[]::new);

            assertThat(expectedPreferredHosts).hasSize(1);

            final Host globalHost = expectedPreferredHosts[0];

            final List<String> expectedPreferredRegions = Collections.singletonList(globalHost.getDatacenter());

            final Host[] initialExpectedFailoverHosts = initialHosts.stream()
                .filter(host -> !Objects.equals(host.getDatacenter(), globalHost.getDatacenter()))
                .sorted(Comparator.comparing(Host::getDatacenter))
                .toArray(Host[]::new);

            validateOperationalState(
                policy,
                globalHost,
                multiRegionWritesEnabled,
                expectedPreferredRegions,
                expectedPreferredHosts,
                initialExpectedFailoverHosts);

            try (Session session = cluster.connect()) {

                Set<Host> hosts = Collections.emptySet();

                for (int iteration = 0; iteration < 10; iteration++) {

                    testAllStatements(session);
                    hosts = clusterMetadata.getAllHosts();

                    if (hosts.size() == REGIONAL_ENDPOINTS.size()) {
                        break;
                    }
                }

                assertThat(hosts).hasSize(REGIONAL_ENDPOINTS.size());

                final Host[] expectedFailoverHosts = hosts.stream()
                    .filter(host -> !Objects.equals(host.getDatacenter(), globalHost.getDatacenter()))
                    .sorted(Comparator.comparing(Host::getDatacenter))
                    .toArray(Host[]::new);

                validateOperationalState(
                    policy,
                    globalHost,
                    multiRegionWritesEnabled,
                    expectedPreferredRegions,
                    expectedPreferredHosts,
                    expectedFailoverHosts);
            }

        } catch (final AssertionError error) {
            throw error;
        } catch (final Throwable error) {
            fail("Failed due to: " + toJson(error), error);
        }
    }

    @BeforeAll
    static void touchCosmosLoadBalancingPolicy() {
        CosmosLoadBalancingPolicy.builder(); // forces class initialization
    }

    /**
     * Validates the final state of a {@link CosmosLoadBalancingPolicy} instance.
     * <p>
     *
     * This is the state of the instance once the cluster under state is fully operation, usually shortly before or
     * after the first session is created. Our checks now require that regional (failover) hosts have been enumerated.
     * We do a full check on the host failover sequence.
     *
     * @param policy                   The {@link CosmosLoadBalancingPolicy} instance under test.
     * @param globalHost               The {@link Host} representing the global endpoint.
     * @param multiRegionWritesEnabled {@code true} if multi-region writes are enabled.
     * @param expectedPreferredRegions The expected list of preferred regions for read and--if multi-region writes are
     * @param expectedPreferredHosts   The expected list preferred hosts, mapping one-to-one with
     *                                 {@code expectedPreferredRegions}.
     * @param expectedFailoverHosts    The list of hosts not in any of the preferred regions, sorted in alphabetic order
     *                                 by datacenter name.
     */
    private static void validateOperationalState(
        final CosmosLoadBalancingPolicy policy,
        final Host globalHost,
        final boolean multiRegionWritesEnabled,
        final List<String> expectedPreferredRegions,
        final Host[] expectedPreferredHosts,
        final Host[] expectedFailoverHosts) {

        assertThat(policy.getPreferredRegions()).containsExactlyElementsOf(expectedPreferredRegions);

        assertThat(policy.getHostsForReading()).hasSize(expectedPreferredHosts.length + expectedFailoverHosts.length);
        assertThat(policy.getHostsForReading()).startsWith(expectedPreferredHosts);
        assertThat(policy.getHostsForReading()).endsWith(expectedFailoverHosts);

        if (multiRegionWritesEnabled) {
            assertThat(policy.getHostsForWriting()).containsExactlyElementsOf(policy.getHostsForReading());
        } else {
            assertThat(policy.getHostsForWriting()).containsExactly(globalHost);
        }
    }

    // endregion

    // region Privates

    // endregion
}
