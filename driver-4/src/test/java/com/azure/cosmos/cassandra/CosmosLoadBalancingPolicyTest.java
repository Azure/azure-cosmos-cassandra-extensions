// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.azure.cosmos.cassandra.CosmosJson.toJson;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_ADDRESS;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_HOSTNAME;
import static com.azure.cosmos.cassandra.TestCommon.GLOBAL_ENDPOINT_PORT;
import static com.azure.cosmos.cassandra.TestCommon.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.TestCommon.REGIONAL_ENDPOINTS;
import static com.azure.cosmos.cassandra.TestCommon.testAllStatements;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test verifies that the {@link CosmosLoadBalancingPolicy} class routes requests correctly.
 * <h3>
 * Preconditions</h3>
 * <ol>
 * <li> A Cosmos DB Cassandra API account is required. It should have at least two regions with multi-region writes
 * enabled. A number of system or--alternatively--environment variables must be set. See
 * {@code src/test/resources/application.conf} and {@link TestCommon} for a complete list. Their use and meaning should
 * be apparent from the relevant sections of the configuration and code.
 * </ol>
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public final class CosmosLoadBalancingPolicyTest {

    // region Fields

    static final Logger LOG = LoggerFactory.getLogger(CosmosLoadBalancingPolicyTest.class);
    private static final int TIMEOUT_IN_SECONDS = 300;

    // endregion

    // region Methods

    /**
     * Prints the parameters for this test class on {@link System#out}.
     */
    @BeforeAll
    public static void init() {
        try {
            TestCommon.printTestParameters();
        } catch (final Throwable error) {
            LOG.error("error: ", error);
            throw error;
        }
    }

    /**
     * Logs the name of each test before it is executed.
     *
     * @param info Test info.
     */
    @BeforeEach
    public void logTestName(final TestInfo info) {
        TestCommon.logTestName(info, LOG);
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
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", justification = "False alarm on Java 11")
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithPreferredRegions(final boolean multiRegionWritesEnabled) {

        final DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(
                CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES,
                multiRegionWritesEnabled)
            .withStringList(
                CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS,
                PREFERRED_REGIONS)
            .build();

        LOG.info("DriverConfiguration({})", toJson(configLoader.getInitialConfig().getDefaultProfile().entrySet()));

        try (CqlSession session = this.connect(configLoader, multiRegionWritesEnabled)) {

            final DriverContext context = session.getContext();
            final String profileName = context.getConfig().getDefaultProfile().getName();
            final CosmosLoadBalancingPolicy policy =
                (CosmosLoadBalancingPolicy) context.getLoadBalancingPolicy(profileName);

            final InetSocketAddress globalEndpointAddress = new InetSocketAddress(
                GLOBAL_ENDPOINT_HOSTNAME,
                GLOBAL_ENDPOINT_PORT);

            // This is the expected state of the instance immediately after a cluster is built, before the first session
            // is created. Starting out we've sometimes got just one host: the global host with a control channel and a
            // newly-created request pool. Our checks in this phase do not require that all regional (failover) hosts
            // have been enumerated.

            final Map<UUID, Node> initialNodes = session.getMetadata().getNodes();

            final Node[] expectedPreferredNodes = initialNodes.values().stream()
                .filter(node -> node.getEndPoint().resolve().equals(globalEndpointAddress))
                .toArray(Node[]::new);

            assertThat(expectedPreferredNodes).hasSize(1);
            assertThat(expectedPreferredNodes[0].getDatacenter()).isNotNull();

            final Node globalHost = expectedPreferredNodes[0];
            final String globalDatacenter = globalHost.getDatacenter();

            final List<String> expectedPreferredRegions = new ArrayList<>(PREFERRED_REGIONS);

            if (!PREFERRED_REGIONS.contains(globalDatacenter)) {
                expectedPreferredRegions.add(globalDatacenter);
            }

            final Node[] initialExpectedPreferredNodes = initialNodes.values().stream()
                .filter(node -> {
                    final String datacenter = node.getDatacenter();
                    assertThat(datacenter).isNotNull();
                    return expectedPreferredRegions.contains(datacenter);
                })
                .sorted(Comparator.comparingInt(node -> expectedPreferredRegions.indexOf(node.getDatacenter())))
                .toArray(Node[]::new);

            final Node[] initialExpectedFailoverNodes = initialNodes.values().stream()
                .filter(node -> {
                    final String datacenter = node.getDatacenter();
                    assertThat(datacenter).isNotNull();
                    return !expectedPreferredRegions.contains(datacenter);
                })
                .sorted(Comparator.comparing(Node::getDatacenter))
                .toArray(Node[]::new);

            validateOperationalState(
                policy,
                globalHost,
                multiRegionWritesEnabled,
                expectedPreferredRegions,
                initialExpectedPreferredNodes,
                initialExpectedFailoverNodes);

                // Here we check after the cluster is fully operation, usually shortly before or shortly after the
                // first session is created. Our checks now require that regional (failover) hosts are fully
                // enumerated. We do a full check on the host failover sequence.

                final Map<UUID, Node> nodes = getAllRegionalNodes(session);

                final Node[] expectedPreferredHosts = nodes.values().stream()
                    .filter(host -> expectedPreferredRegions.contains(host.getDatacenter()))
                    .sorted(Comparator.comparingInt(x -> expectedPreferredRegions.indexOf(x.getDatacenter())))
                    .toArray(Node[]::new);

                assertThat(expectedPreferredHosts).hasSize(expectedPreferredRegions.size());

                final Node[] expectedFailoverHosts = nodes.values().stream()
                    .filter(node -> {
                        final String datacenter = node.getDatacenter();
                        assertThat(datacenter).isNotNull();
                        return !expectedPreferredRegions.contains(datacenter);
                    })
                    .sorted(Comparator.comparing(Node::getDatacenter))
                    .toArray(Node[]::new);

                validateOperationalState(
                    policy,
                    globalHost,
                    multiRegionWritesEnabled,
                    expectedPreferredRegions,
                    expectedPreferredHosts,
                    expectedFailoverHosts);

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
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", justification = "False alarm on Java 11")
    @ParameterizedTest
    @Tag("checkin")
    @Tag("integration")
    @Timeout(TIMEOUT_IN_SECONDS)
    @ValueSource(booleans = { false, true })
    public void testWithoutPreferredRegions(final boolean multiRegionWritesEnabled) {

        final DriverConfigLoader configLoader = newProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(
                CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES,
                multiRegionWritesEnabled)
            .withStringList(
                CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS,
                Collections.emptyList())
            .build();

        LOG.info("DriverConfiguration({})", toJson(configLoader.getInitialConfig().getDefaultProfile().entrySet()));

        try (CqlSession session = this.connect(configLoader, multiRegionWritesEnabled)) {

            final DriverContext context = session.getContext();
            final String name = context.getConfig().getDefaultProfile().getName();
            final CosmosLoadBalancingPolicy policy = (CosmosLoadBalancingPolicy) context.getLoadBalancingPolicy(name);

            final InetSocketAddress globalEndpointAddress = new InetSocketAddress(
                GLOBAL_ENDPOINT_HOSTNAME,
                GLOBAL_ENDPOINT_PORT);

            // This is the expected state of the instance immediately after a cluster is built, before the first session
            // is created. Starting out we've sometimes got just one host: the global host with a control channel and a
            // newly-created request pool. Our checks in this phase do not require that all regional (failover)
            // regionalHosts
            // have been enumerated.

            final Map<UUID, Node> initialNodes = session.getMetadata().getNodes();

            final Node[] expectedPreferredNodes = initialNodes.values().stream()
                .filter(node -> node.getEndPoint().resolve().equals(globalEndpointAddress))
                .toArray(Node[]::new);

            assertThat(expectedPreferredNodes).hasSize(1);
            assertThat(expectedPreferredNodes[0].getDatacenter()).isNotNull();

            final Node globalHost = expectedPreferredNodes[0];
            final String globalDatacenter = globalHost.getDatacenter();

            final List<String> expectedPreferredRegions = Collections.singletonList(globalHost.getDatacenter());

            final Node[] initialExpectedFailoverNodes = initialNodes.values().stream()
                .filter(node -> {
                    final String datacenter = node.getDatacenter();
                    assertThat(datacenter).isNotNull();
                    return !datacenter.equals(globalDatacenter);
                })
                .sorted(Comparator.comparing(Node::getDatacenter))
                .toArray(Node[]::new);

            validateOperationalState(
                policy,
                globalHost,
                multiRegionWritesEnabled,
                expectedPreferredRegions,
                expectedPreferredNodes,
                initialExpectedFailoverNodes);

            // Here we check after the cluster is fully operation, usually shortly before or shortly after the
            // first session is created. Our checks now require that regional (failover) nodes are fully
            // enumerated. We do a full check on the host failover sequence.

            final Map<UUID, Node> regionalNodes = getAllRegionalNodes(session);

            final Node[] expectedFailoverNodes = regionalNodes.values().stream()
                .filter(node -> {
                    final String datacenter = node.getDatacenter();
                    assertThat(datacenter).isNotNull();
                    return !datacenter.equals(globalDatacenter);
                })
                .sorted(Comparator.comparing(Node::getDatacenter))
                .toArray(Node[]::new);

            validateOperationalState(
                policy,
                globalHost,
                multiRegionWritesEnabled,
                expectedPreferredRegions,
                expectedPreferredNodes,
                expectedFailoverNodes);

        } catch (final AssertionError error) {
            throw error;
        } catch (final Throwable error) {
            fail("Failed due to: " + toJson(error), error);
        }
    }

    // endregion

    // region Privates

    private static DriverConfigLoader checkState(final DriverConfigLoader configLoader) {

        final DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();

        assertThat(profile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS))
            .isEqualTo(CosmosLoadBalancingPolicy.class.getName());

        assertThat(profile.getString(DefaultDriverOption.RECONNECTION_POLICY_CLASS))
            .isEqualTo(ConstantReconnectionPolicy.class.getName());

        assertThat(profile.getString(DefaultDriverOption.RETRY_POLICY_CLASS))
            .isEqualTo(CosmosRetryPolicy.class.getName());

        return configLoader;
    }

    private static CqlSession checkState(final CqlSession session, final boolean multiRegionWrites) {

        final DriverContext driverContext = session.getContext();
        final Map<UUID, Node> nodes = session.getMetadata().getNodes();
        final DriverExecutionProfile profile = driverContext.getConfig().getDefaultProfile();

        // Check that the driver has got the nodes we expect: one per region because we're connected to Cosmos DB

        assertThat(nodes.values().stream().map(node -> node.getEndPoint().resolve())).containsAll(REGIONAL_ENDPOINTS);
        assertThat(nodes.size()).isEqualTo(REGIONAL_ENDPOINTS.size());

        // Check that we've got the load balancing policy we think we have

        final LoadBalancingPolicy loadBalancingPolicy = driverContext.getLoadBalancingPolicy(profile.getName());

        assertThat(loadBalancingPolicy).isExactlyInstanceOf(CosmosLoadBalancingPolicy.class);

        final CosmosLoadBalancingPolicy cosmosLoadBalancingPolicy = (CosmosLoadBalancingPolicy) loadBalancingPolicy;

        assertThat(cosmosLoadBalancingPolicy.getMultiRegionWritesEnabled()).isEqualTo(multiRegionWrites);

        final List<String> configuredPreferredRegions = profile.getStringList(CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS);

        final List<String> preferredReadRegions = cosmosLoadBalancingPolicy.getPreferredReadRegions();
        final List<String> preferredWriteRegions = cosmosLoadBalancingPolicy.getPreferredWriteRegions();

        assertThat(preferredReadRegions.size()).isBetween(
            configuredPreferredRegions.size(),
            configuredPreferredRegions.size() + 1);

        assertThat(preferredWriteRegions.size()).isBetween(
            configuredPreferredRegions.size(),
            configuredPreferredRegions.size() + 1);

        assertThat(preferredReadRegions).hasSize(preferredWriteRegions.size());

        final List<Node> nodesForReading = cosmosLoadBalancingPolicy.getNodesForReading();

        if (preferredReadRegions.size() == configuredPreferredRegions.size()) {
            final SocketAddress address = getSocketAddress(nodesForReading, preferredReadRegions.size() - 1);
            assertThat(address).isEqualTo(GLOBAL_ENDPOINT_ADDRESS);
        }

        if (multiRegionWrites) {
            assertThat(cosmosLoadBalancingPolicy.getNodesForWriting()).isEqualTo(nodesForReading);
        } else {
            final List<Node> nodesForWriting = cosmosLoadBalancingPolicy.getNodesForWriting();
            if (preferredReadRegions.size() == configuredPreferredRegions.size()) {
                final SocketAddress address = getSocketAddress(nodesForWriting, 0);
                assertThat(address).isEqualTo(GLOBAL_ENDPOINT_ADDRESS);
            }
        }

        LOG.info("[{}] connected to nodes {} with load balancing policy {}",
            toJson(session.getName()),
            toJson(nodes),
            loadBalancingPolicy);

        return session;
    }

    private static SocketAddress getSocketAddress(List<Node> nodesForWriting, int i) {
        return nodesForWriting.get(i).getEndPoint().resolve();
    }

    @NonNull
    private CqlSession connect(@NonNull final DriverConfigLoader configLoader, final boolean multiRegionWrites)
        throws InterruptedException {

        final CqlSession session = CqlSession.builder().withConfigLoader(checkState(configLoader)).build();

        try {
            Thread.sleep(5_000L); // Gives the session time to enumerate peers and initialize all channel pools
            return checkState(session, multiRegionWrites);
        } catch (final Throwable error) {
            session.close();
            throw error;
        }
    }

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
    private static Map<UUID, Node> getAllRegionalNodes(final CqlSession session) {

        final Metadata metadata = session.getMetadata();
        Map<UUID, Node> nodes = Collections.emptyMap();
        int iterations = 0;

        try {

            do {
                testAllStatements(session);
                nodes = metadata.getNodes();
            } while (nodes.size() != REGIONAL_ENDPOINTS.size() && ++iterations < 10);

            return nodes;

        } finally {

            assertThat(nodes).hasSize(REGIONAL_ENDPOINTS.size());

            assertThat(nodes.values()).are(new Condition<>(
                node -> REGIONAL_ENDPOINTS.contains((InetSocketAddress) node.getEndPoint().resolve()),
                "regional endpoint"));

            LOG.info("[{}] regional endpoints enumerated in iteration {}: {}",
                session.getName(),
                iterations,
                toJson(nodes));
        }
    }

    private static ProgrammaticDriverConfigLoaderBuilder newProgrammaticDriverConfigLoaderBuilder() {
        return DriverConfigLoader.programmaticBuilder();
    }

    /**
     * Validates the operational state of a {@link CosmosLoadBalancingPolicy} instance.
     *
     * @param policy                   The {@link CosmosLoadBalancingPolicy} instance under test.
     * @param globalNode               The {@link Node} representing the global endpoint.
     * @param multiRegionWritesEnabled {@code true} if multi-region writes are enabled.
     * @param expectedPreferredRegions The expected list of preferred regions for read and--if multi-region writes are
     * @param expectedPreferredNodes   The expected list preferred hosts, mapping one-to-one with {@code
     *                                 expectedPreferredRegions}.
     * @param expectedFailoverNodes    The list of hosts not in any of the preferred regions, sorted in alphabetic order
     *                                 by datacenter name.
     */
    private static void validateOperationalState(
        final CosmosLoadBalancingPolicy policy,
        final Node globalNode,
        final boolean multiRegionWritesEnabled,
        final List<String> expectedPreferredRegions,
        final Node[] expectedPreferredNodes,
        final Node[] expectedFailoverNodes) {

        assertThat(policy.getPreferredReadRegions()).containsExactlyElementsOf(expectedPreferredRegions);

        assertThat(policy.getNodesForReading()).hasSize(expectedPreferredNodes.length + expectedFailoverNodes.length);
        assertThat(policy.getNodesForReading()).startsWith(expectedPreferredNodes);
        assertThat(policy.getNodesForReading()).endsWith(expectedFailoverNodes);

        if (multiRegionWritesEnabled) {

            assertThat(policy.getNodesForWriting()).containsExactlyElementsOf(policy.getNodesForReading());

        } else {

            final List<Node> nodesForWriting = policy.getNodesForWriting();
            assertThat(nodesForWriting).hasSize(expectedPreferredNodes.length + expectedFailoverNodes.length);

            if (nodesForWriting.get(0).equals(globalNode)) {
                for (int i = 1; i < expectedPreferredNodes.length; i++) {
                    assertThat(nodesForWriting.get(i)).isEqualTo(expectedPreferredNodes[i - 1]);
                }
            } else {
                assertThat(nodesForWriting).startsWith(expectedPreferredNodes);
            }

            assertThat(nodesForWriting).endsWith(expectedFailoverNodes);
        }
    }

    // endregion
}
