// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.azure.cosmos.cassandra.CosmosJson.toJson;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_ADDRESS;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_HOSTNAME;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_PORT;
import static com.azure.cosmos.cassandra.TestCommon.PASSWORD;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.REGIONAL_ENDPOINTS;
import static com.azure.cosmos.cassandra.TestCommon.REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.USERNAME;
import static com.azure.cosmos.cassandra.TestCommon.cosmosClusterBuilder;
import static com.azure.cosmos.cassandra.TestCommon.testAllStatements;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test verifies that the {@link CosmosLoadBalancingPolicy} class routes requests correctly.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li> A Cosmos DB Cassandra API account is required. It should have at least two regions with multi-region writes
 * enabled. A number of system or--alternatively--environment variables must be set. See {@link TestCommon} for a
 * complete list. Their use and meaning should be apparent from the relevant sections of the configuration and code.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public final class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);
    public static final long PAUSE_MILLIS = 5_000L;
    private static final int TIMEOUT_IN_SECONDS = 30;

    // endregion

    // region Methods

    @BeforeAll
    public static void init() {
        TestCommon.printTestParameters();
        CosmosLoadBalancingPolicy.builder(); // forces class initialization
    }

    @BeforeEach
    public void logTestName(final TestInfo info) {
        TestCommon.logTestName(info, LOG);
    }

    public static Stream<Arguments> provideCosmosLoadBalancingPolicy() {
        return Stream.of(true, false).flatMap(multiRegionWritesEnabled -> Stream.concat(
            Stream.of(
                Arguments.of(CosmosLoadBalancingPolicy.builder()
                    .withMultiRegionWrites(multiRegionWritesEnabled)
                    .build()),
                Arguments.of(CosmosLoadBalancingPolicy.builder()
                    .withMultiRegionWrites(multiRegionWritesEnabled)
                    .withPreferredRegions(REGIONS)
                    .build())),
            REGIONS.stream().map(region ->
                Arguments.of(CosmosLoadBalancingPolicy.builder()
                    .withMultiRegionWrites(multiRegionWritesEnabled)
                    .withPreferredRegions(Collections.singletonList(region))
                    .build()))));
    }

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
     * Read requests are handled the same whether or not multi-region writes are enabled. We do not test failover
     * scenarios here. We simply check that all requests are sent to the first preferred region when multi-region writes
     * are enabled. When multi-region writes are disabled we verify that all read requests are sent to the first
     * preferred region and that all write requests are sent to the global endpoint address.
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

            final CosmosLoadBalancingPolicy policy = getCosmosLoadBalancingPolicy(
                cluster,
                multiRegionWritesEnabled,
                PREFERRED_REGIONS);

            final Metadata clusterMetadata = cluster.getMetadata();

            final InetSocketAddress globalEndpointAddress = new InetSocketAddress(
                GLOBAL_ENDPOINT_HOSTNAME,
                GLOBAL_ENDPOINT_PORT);

            // This is the expected state of the instance immediately after a cluster is built, before the first session
            // is created. Starting out we've sometimes got just one host: the global host with a control channel and a
            // newly-created request pool. Our checks in this phase do not require that all regional (failover) hosts
            // have been enumerated.

            final Set<Host> initialHosts = clusterMetadata.getAllHosts();

            final Host primary = initialHosts.stream()
                .filter(host -> host.getBroadcastRpcAddress().equals(globalEndpointAddress))
                .toArray(Host[]::new)[0];

            final List<String> expectedPreferredRegions = new ArrayList<>(PREFERRED_REGIONS);

            if (!PREFERRED_REGIONS.contains(primary.getDatacenter())) {
                expectedPreferredRegions.add(primary.getDatacenter());
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
                primary,
                multiRegionWritesEnabled,
                expectedPreferredRegions,
                initialExpectedPreferredHosts,
                initialExpectedFailoverHosts);

            try (Session session = cluster.connect()) {

                // Here we check after the cluster is fully operation, usually shortly before or shortly after the
                // first session is created. Our checks now require that regional (failover) hosts are fully
                // enumerated. We do a full check on the host failover sequence.

                final Set<Host> hosts = getAllRegionalHosts(session);

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
                    primary,
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

        final List<String> preferredRegions = Collections.emptyList();

        final Cluster.Builder builder = cosmosClusterBuilder(
            CosmosLoadBalancingPolicy.builder()
                .withMultiRegionWrites(multiRegionWritesEnabled)
                .withPreferredRegions(Collections.emptyList())
                .build())
            .withClusterName("testWithoutPreferredRegions.multiRegionWritesEnabled=" + multiRegionWritesEnabled)
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD);

        try (Cluster cluster = builder.build()) {

            final CosmosLoadBalancingPolicy policy = getCosmosLoadBalancingPolicy(
                cluster,
                multiRegionWritesEnabled,
                preferredRegions);

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

                // Here we check after the cluster is fully operational, usually shortly before or shortly after the
                // first session is created. Our checks now require that regional (failover) hosts are fully
                // enumerated. We do a full check on the host failover sequence.

                final Set<Host> hosts = getAllRegionalHosts(session);

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

    @ParameterizedTest
    @Tag("checkin")
    @Timeout(TIMEOUT_IN_SECONDS)
    @MethodSource("provideCosmosLoadBalancingPolicy")
    void testPolicyCorrectness(final CosmosLoadBalancingPolicy policy) {

        final List<String> preferredRegionsAsSpecified = policy.getPreferredReadRegions();

        try (final Cluster cluster = cosmosClusterBuilder(policy, CosmosRetryPolicy.defaultPolicy())
            .withClusterName("testPolicyCorrectness")
            .addContactPoint(GLOBAL_ENDPOINT_HOSTNAME)
            .withCredentials(USERNAME, PASSWORD)
            .build()) {

            // Driver Behavior:
            // Cluster::init calls CosmosLoadBalancingPolicy::init with the list of contact points, minus any that are
            // determined to be down or to have been removed. Cluster::init will also have initiated a call to
            // ControlConnection::refreshNodeListAndTokenMap. Hence, additional hosts may be present following return
            // from our call to Cluster::init. The work of querying system.local and system.peers is performed in
            // ControlConnection::updateInfo.

            // Expectation:
            // - CosmosLoadBalancingPolicy::init is called with the host representing the GLOBAL_ENDPOINT_HOSTNAME
            // - Cluster::init will have initiated a call to ControlConnection::refreshNodeListAndTokenMap
            // - On return from our call to Cluster::init the host representing the GLOBAL_ENDPOINT_HOSTNAME--
            //   the primary--will be up. Other hosts may have been enumerated. Some may be up and others not.

            cluster.init();

            Set<Host> hosts = cluster.getMetadata().getAllHosts();

            final Host primary = hosts.stream()
                .filter(host -> host.getEndPoint().resolve().equals(GLOBAL_ENDPOINT_ADDRESS))
                .findFirst()
                .orElse(null);

            assertThat(primary).isNotNull();
            assertThat(primary.isUp()).isTrue();

            final Object source = cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
            assertThat(source).isSameAs(policy);

            this.checkPreferredRegions(policy, primary, preferredRegionsAsSpecified);
            assertThat(policy.getHostsForReading()).contains(primary);
            assertThat(policy.getHostsForWriting()).contains(primary);

            // Expectation:
            // - CosmosLoadBalancingPolicy::add is called once for each host returned from the query to system.peers.
            // - On return from our call to Cluster::connect, all hosts known to the Cluster and that the Cluster
            //   has determined to be up will be represented in order of preferred region by CosmosLoadBalancingPolicy.
            // - This process can take time. We allow ample time for the driver to enumerate and bring all hosts up.

            cluster.connect();  // we don't need the session

            hosts = cluster.getMetadata().getAllHosts();

            if (hosts.size() < REGIONAL_ENDPOINTS.size()) {
                LOG.info(
                    "Pausing {} ms to allow time for DataStax Java Driver {} to enumerate all hosts",
                    PAUSE_MILLIS,
                    Cluster.getDriverVersion());
                try {
                    Thread.sleep(PAUSE_MILLIS);
                } catch (final InterruptedException error) {
                    // ignore
                }
                hosts = cluster.getMetadata().getAllHosts();
            }

            this.checkPreferredRegions(policy, primary, preferredRegionsAsSpecified);
            final int up = hosts.stream().map(host -> host.isUp() ? 1 : 0).reduce(0, Integer::sum);

            if (up < hosts.size()) {
                LOG.info(
                    "Pausing {} ms to allow time for DataStax Java Driver {} to establish that all hosts are up",
                    PAUSE_MILLIS,
                    Cluster.getDriverVersion());
                try {
                    Thread.sleep(PAUSE_MILLIS);
                } catch (final InterruptedException error) {
                    // ignore
                }
                hosts = cluster.getMetadata().getAllHosts().stream().filter(Host::isUp).collect(Collectors.toSet());
            }

            final List<Host> hostsForReading = policy.getHostsForReading();
            final List<Host> hostsForWriting = policy.getHostsForWriting();

            assertThat(hostsForReading).hasSameElementsAs(hosts);

            if (policy.getMultiRegionWritesEnabled()) {
                assertThat(hostsForWriting).containsExactlyElementsOf(hostsForReading);
            } else {
                assertThat(hostsForWriting).hasSameElementsAs(hostsForReading);
                assertThat(hostsForWriting).startsWith(primary);
            }

            assertThat(hosts.size()).isEqualTo(REGIONAL_ENDPOINTS.size());
        }
    }

    private void checkPreferredRegions(
        final CosmosLoadBalancingPolicy policy,
        final Host primary,
        final List<String> preferredRegionsAsSpecified) {

        final List<String> preferredReadRegions = policy.getPreferredReadRegions();
        final String primaryRegion = primary.getDatacenter();

        if (preferredRegionsAsSpecified.isEmpty()) {
            assertThat(preferredReadRegions).containsExactly(primaryRegion);
        } else {
            if (preferredRegionsAsSpecified.contains(primaryRegion)) {
                assertThat(preferredReadRegions).containsExactlyElementsOf(preferredRegionsAsSpecified);
            } else {
                assertThat(preferredReadRegions).startsWith(preferredRegionsAsSpecified.toArray(new String[0]));
                assertThat(preferredReadRegions.size()).isEqualTo(preferredRegionsAsSpecified.size() + 1);
                assertThat(preferredReadRegions).endsWith(primaryRegion);
            }
        }
        if (policy.getMultiRegionWritesEnabled()) {
            assertThat(policy.getPreferredWriteRegions()).containsExactlyElementsOf(preferredReadRegions);
        } else {

            final List<String> preferredWriteRegions = policy.getPreferredWriteRegions();

            if (preferredRegionsAsSpecified.isEmpty()) {
                assertThat(preferredWriteRegions).containsExactly(primaryRegion);
            } else {

                final String[] expectedPreferredWriteRegions = preferredRegionsAsSpecified.stream()
                    .filter(region -> !region.equals(primaryRegion))
                    .toArray(String[]::new);

                assertThat(preferredWriteRegions).startsWith(primaryRegion).endsWith(expectedPreferredWriteRegions);
                assertThat(preferredWriteRegions.size()).isEqualTo(expectedPreferredWriteRegions.length + 1);
            }
        }
    }

    // endregion

    // region Privates

    /**
     * Runs tests until all regional hosts are enumerated by the driver or the calling test times out.
     * <p>
     * The {@link TestCommon#REGIONAL_ENDPOINTS} list is used to determine whether all regional hosts have been
     * enumerated. You must ensure that the list of {@link TestCommon#REGIONAL_ENDPOINTS} is accurate.
     *
     * @param session The current session.
     *
     * @return The set of regional hosts obtained from the driver. The number of hosts returned is guaranteed to be
     * equal to the size of {@link TestCommon#REGIONAL_ENDPOINTS}.
     */
    private static Set<Host> getAllRegionalHosts(final Session session) {

        final Metadata metadata = session.getCluster().getMetadata();
        Set<Host> hosts = Collections.emptySet();
        int iterations = 1;

        try {

            do {
                testAllStatements(session);
                hosts = metadata.getAllHosts();
            } while (hosts.size() != REGIONAL_ENDPOINTS.size() && ++iterations <= 5);

            return hosts;

        } finally {

            final Stream<InetSocketAddress> addresses = hosts.stream().map(host -> host.getEndPoint().resolve());
            final Set<Host> finalHosts = hosts;

            assertThat(hosts).withFailMessage(() ->
                    "Expected all hosts to match a regional endpoint: {"
                        + "\"hosts\":" + toJson(finalHosts) + ","
                        + "\"host-addresses\":" + toJson(addresses.collect(Collectors.toList())) + ","
                        + "\"regional-endpoint-addresses\":" + toJson(REGIONAL_ENDPOINTS) + "}")
                .allMatch(host -> REGIONAL_ENDPOINTS.contains(host.getEndPoint().resolve()));

            LOG.info(
                "[{}] {} regional endpoints enumerated in iteration {}: {}",
                session.getCluster().getClusterName(),
                hosts.size(),
                iterations,
                toJson(hosts));
        }
    }

    private static CosmosLoadBalancingPolicy getCosmosLoadBalancingPolicy(
        @NonNull final Cluster cluster,
        final boolean multiRegionWrites,
        @NonNull final List<String> configuredPreferredRegions) {

        final LoadBalancingPolicy policy = cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertThat(policy).isExactlyInstanceOf(CosmosLoadBalancingPolicy.class);

        final CosmosLoadBalancingPolicy cosmosLoadBalancingPolicy = (CosmosLoadBalancingPolicy) policy;
        assertThat(cosmosLoadBalancingPolicy.getMultiRegionWritesEnabled()).isEqualTo(multiRegionWrites);

        final List<String> preferredReadRegions = cosmosLoadBalancingPolicy.getPreferredReadRegions();
        assertThat(preferredReadRegions).containsExactlyElementsOf(configuredPreferredRegions);

        final List<String> preferredWriteRegions = cosmosLoadBalancingPolicy.getPreferredWriteRegions();
        assertThat(preferredWriteRegions).containsExactlyElementsOf(configuredPreferredRegions);

        return cosmosLoadBalancingPolicy;
    }

    /**
     * Validates the operational state of a {@link CosmosLoadBalancingPolicy} instance.
     *
     * @param policy                   The {@link CosmosLoadBalancingPolicy} instance under test.
     * @param primary                  The {@link Host} representing the primary region.
     * @param multiRegionWritesEnabled {@code true} if multi-region writes are enabled.
     * @param expectedPreferredRegions The expected list of preferred regions for read and--if multi-region writes are
     * @param expectedPreferredHosts   The expected list preferred hosts, mapping one-to-one with {@code
     *                                 expectedPreferredRegions}.
     * @param expectedFailoverHosts    The list of hosts not in any of the preferred regions, sorted in alphabetic order
     *                                 by datacenter name.
     */
    private static void validateOperationalState(
        final CosmosLoadBalancingPolicy policy,
        final Host primary,
        final boolean multiRegionWritesEnabled,
        final List<String> expectedPreferredRegions,
        final Host[] expectedPreferredHosts,
        final Host[] expectedFailoverHosts) {

        assertThat(policy.getPreferredReadRegions()).containsExactlyElementsOf(expectedPreferredRegions);

        final List<Host> hostsForReading = policy.getHostsForReading();

        assertThat(hostsForReading).hasSize(expectedPreferredHosts.length + expectedFailoverHosts.length);
        assertThat(hostsForReading).startsWith(expectedPreferredHosts);
        assertThat(hostsForReading).endsWith(expectedFailoverHosts);

        if (multiRegionWritesEnabled) {
            assertThat(policy.getHostsForWriting()).containsExactlyElementsOf(hostsForReading);
        } else {
            final List<Host> hostsForWriting = policy.getHostsForWriting();
            assertThat(hostsForWriting).startsWith(primary);
            assertThat(hostsForWriting).endsWith(hostsForReading.stream()
                .filter(host -> !host.getDatacenter().equals(primary.getDatacenter()))
                .toArray(Host[]::new));
            assertThat(hostsForWriting.size()).isEqualTo(hostsForReading.size());
        }
    }

    // endregion
}
