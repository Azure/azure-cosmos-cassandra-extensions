// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
 * specification of load balancing policy options. This test should be run against single-region, multi-region, and
 * multi-master accounts.
 */
public class ApplicationCommandLineRunnerTest {

    // region Fields

    private static final List<String> COMMAND;

    private static final List<String> EXPECTED_OUTPUT;

    private static final String JAR = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.jar",
        "AZURE_COSMOS_CASSANDRA_JAR",
        System.getProperty("java.classpath"));

    private static final String JAVA = Paths.get(System.getProperty("java.home"), "bin", "java").toString();

    private static final String JAVA_OPTIONS = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.java.options",
        "AZURE_COSMOS_CASSANDRA_JAVA_OPTIONS",
        "");

    private static final String LOG_PATH = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.log-path",
        "AZURE_COSMOS_CASSANDRA_LOG_PATH",
        Paths.get(System.getProperty("user.home"), ".local", "var", "log").toString());

    private static final long TIMEOUT_IN_MINUTES = 2;

    static {

        final List<String> command = new ArrayList<>();

        command.add(JAVA);
        command.addAll(Arrays.asList(JAVA_OPTIONS.split("\\s+")));
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

        try (final CqlSession session = CqlSession.builder().build()) {
            session.execute(SimpleStatement.newInstance("CREATE KEYSPACE IF NOT EXISTS "
                + "azure_cosmos_cassandra_driver_4_examples WITH "
                + "REPLICATION={"
                + "'class':'SimpleStrategy',"
                + "   'replication_factor':4"
                + "} AND "
                + "cosmosdb_provisioned_throughput=100000").setConsistencyLevel(ConsistencyLevel.ALL));
        } catch (final Throwable error) {
            fail("could not recreate table azure_cosmos_cassandra_driver_4_examples.people", error);
        }
    }

    /**
     * Runs the spring-boot-app and ensures that it completes with status code zero.
     * <p>
     * CosmosLoadBalancingPolicy is configured with and without multi-region writes.
     */
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void run(final boolean multiRegionWrites) {
        final ProcessBuilder builder = new ProcessBuilder();
        builder.environment().put("AZURE_COSMOS_CASSANDRA_MULTI_REGION_WRITE", "true");
        this.exec("run.withMultiRegionWrites(" + multiRegionWrites + ")", builder);
    }

    // endregion

    // region Privates

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void exec(final String testName, final ProcessBuilder processBuilder) {

        final String baseFilename = "azure-cosmos-cassandra-spring-boot-app." + testName;
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

    // endregion
}
