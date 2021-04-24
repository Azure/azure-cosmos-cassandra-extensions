// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.Fail.fail;

/**
 * Verifies that the spring-boot-app example runs and exits with status code zero.
 * <p>
 * Three permutations of load balancing policy are tested to ensure there are no surprises based on a valid
 * specification of load balancing policy options. This test should be run against single-region, multi-region,
 * and multi-master accounts.
 */
public class ApplicationCommandLineRunnerTest {

    // region Fields

    private static final List<String> COMMAND;

    private static final String CONTACT_POINT = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.contact-point",
        "AZURE_COSMOS_CASSANDRA_CONTACT_POINT",
        null);

    private static final List<String> EXPECTED_OUTPUT;

    private static final String JAR = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.jar",
        "AZURE_COSMOS_CASSANDRA_JAR",
        System.getProperty("java.classpath"));

    private static final String JAVA = Paths.get(System.getProperty("java.home"), "bin", "java").toString();

    private static final String LOG_PATH = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.log-path",
        "AZURE_COSMOS_CASSANDRA_LOG_PATH",
        Paths.get(System.getProperty("user.home"), ".local", "var", "log").toString());

    private static final String OPTIONS = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.java.options",
        "AZURE_COSMOS_CASSANDRA_JAVA_OPTIONS",
        "");

    private static final String PASSWORD = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.password",
        "AZURE_COSMOS_CASSANDRA_PASSWORD",
        null);

    private static final long TIMEOUT_IN_MINUTES = 2;

    private static final String USERNAME = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.username",
        "AZURE_COSMOS_CASSANDRA_USERNAME",
        null);

    static {

        final List<String> command = new ArrayList<>();

        command.add(JAVA);
        command.addAll(Arrays.asList(OPTIONS.split("\\s+")));
        command.add("-jar");
        command.add(JAR);

        COMMAND = Collections.unmodifiableList(command);

        List<String> expectedOutput = null;

        try (final BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                Objects.requireNonNull(ApplicationCommandLineRunner.class
                    .getClassLoader()
                    .getResourceAsStream("expected.output")), StandardCharsets.UTF_8))) {
            expectedOutput = reader.lines().collect(Collectors.toList());
        } catch (final IOException error) {
            expectedOutput = null;
        }

        EXPECTED_OUTPUT = expectedOutput;
    }

    // endregion

    // region Methods

    @BeforeAll
    public static void checkExpectedOutputAndJar() {
        assertThat(EXPECTED_OUTPUT).isNotEmpty();
        assertThat(JAR).isNotBlank();
        assertThat(Paths.get(JAR)).exists();
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void recreateKeyspace() {

        final CqlSessionBuilder builder = CqlSession.builder()
            .withApplicationName(ApplicationCommandLineRunnerTest.class.getName())
            .addContactEndPoint(parseEndPoint(CONTACT_POINT))
            .withAuthCredentials(USERNAME, PASSWORD);

        try (final CqlSession session = builder.build()) {

            session.execute("DROP KEYSPACE IF EXISTS azure_cosmos_cassandra_driver_4_examples");

            session.execute("CREATE KEYSPACE azure_cosmos_cassandra_driver_4_examples WITH REPLICATION={"
                + "'class':'SimpleStrategy',"
                + "   'replication_factor':4"
                + "}");

            session.execute("CREATE TABLE azure_cosmos_cassandra_driver_4_examples.people ("
                + "first_name text,"
                + "birth_date timestamp,"
                + "uuid uuid,"
                + "last_name text,"
                + "occupation text,"
                + "PRIMARY KEY (first_name, birth_date, uuid))"
                + "WITH CLUSTERING ORDER BY (birth_date ASC, uuid DESC) AND default_time_to_live=3600;");

        } catch (final Throwable error) {
            fail("could not recreate keyspace azure_cosmos_cassandra_driver_4_examples due to %s", error);
        }
    }

    /**
     * Starts the spring-boot-app and ensures that it completes with status code zero.
     * CosmosLoadBalancingPolicy is configured with a global endpoint.
     */
    @Test
    public void withGlobalEndpoint() {

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> environment = builder.environment();

        environment.remove("AZURE_COSMOS_CASSANDRA_READ_DATACENTER");
        environment.remove("AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER");

        this.exec("withGlobalEndpoint", builder);
    }

    /**
     * Starts the spring-boot-app and ensures that it completes with status code zero.
     * CosmosLoadBalancingPolicy is configured with a global endpoint and a read datacenter.
     */
    @Test
    public void withGlobalEndpointAndReadDatacenter() {

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> environment = builder.environment();
        environment.remove("AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER");

        this.exec("withGlobalEndpointAndReadDatacenter", builder);
    }

    /**
     * Starts the spring-boot-app and ensures that it completes with status code zero.
     * CosmosLoadBalancingPolicy is configured with read and write datacenters.
     */
    @Test
    public void withReadDatacenterAndWriteDatacenter() {

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> environment = builder.environment();
        environment.remove("AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT");

        this.exec("withReadDatacenterAndWriteDatacenter", builder);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void exec(final String testName, final ProcessBuilder processBuilder) {

        final String baseFilename = "azure-cosmos-cassandra-spring-boot-app.CosmosLoadBalancingPolicy." + testName;
        final Path logPath = Paths.get(LOG_PATH, baseFilename + ".log");
        final Path outputPath = Paths.get(LOG_PATH, baseFilename + ".output");

        assertThatCode(() -> Files.deleteIfExists(logPath)).doesNotThrowAnyException();
        assertThatCode(() -> Files.deleteIfExists(outputPath)).doesNotThrowAnyException();

        processBuilder.command(COMMAND).environment().put("AZURE_COSMOS_CASSANDRA_LOG_FILE", logPath.toString());

        final Process process;

        try {
            process = processBuilder.start();
        } catch (final Throwable error) {
            fail("failed to execute command '%s' due to %s", COMMAND, error);
            return;
        }

        final List<String> output;

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(),
            StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.toList());
        } catch (final IOException error) {
            fail("failed to execute command '%s' due to %s", COMMAND, error);
            return;
        }

        try (final FileWriter writer = new FileWriter(outputPath.toFile(), StandardCharsets.UTF_8)) {
            for (final String line : output) {
                writer.write(line);
                writer.write('\n');
            }
        } catch (final IOException error) {
            fail("failed to write command output to '%s' due to %s", outputPath, error);
            return;
        }

        try {
            assertThat(process.waitFor(TIMEOUT_IN_MINUTES, TimeUnit.MINUTES)).isTrue();
        } catch (final InterruptedException error) {
            fail("command '%s timed out after %d minutes", COMMAND, TIMEOUT_IN_MINUTES);
            return;
        }

        assertThat(output).startsWith(EXPECTED_OUTPUT.get(0), EXPECTED_OUTPUT.get(1));
        assertThat(output).endsWith(EXPECTED_OUTPUT.get(EXPECTED_OUTPUT.size() - 1));
        assertThat(output.size()).isEqualTo(EXPECTED_OUTPUT.size());

        assertThat(process.exitValue()).isEqualTo(0);
    }

    // endregion

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
    private static String getPropertyOrEnvironmentVariable(
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

    private static EndPoint parseEndPoint(final String value) {

        final int index = value.indexOf(':');

        final String hostname = value.substring(0, index);
        final int port = Integer.parseUnsignedInt(value.substring(index + 1));

        final InetSocketAddress address = new InetSocketAddress(hostname, port);

        return new DefaultEndPoint(address);
    }

    // endregion
}
