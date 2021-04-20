// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.Fail.fail;

/**
 * Verifies that the spring-boot-app example runs and exits with status code zero.
 *
 * Three permutations of load balancing policy are tested to ensure there are no surprises based on a valid
 * specification of load balancing options. This test should be run against single-region, multi-region, and
 * multi-master accounts.
 */
public class ApplicationCommandLineRunnerTest {

    // region Fields

    private static final List<String> COMMAND;

    private static final String JAR = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.jar",
        "AZURE_COSMOS_CASSANDRA_JAR",
        System.getProperty("java.classpath"));

    private static final String JAVA = Paths.get(System.getProperty("java.home"), "bin", "java").toString();

    private static final String OPTIONS = getPropertyOrEnvironmentVariable(
        "azure.cosmos.cassandra.java.options",
        "AZURE_COSMOS_CASSANDRA_JAVA_OPTIONS",
        "");

    private static final long TIMEOUT_IN_MINUTES = 2;

    static {

        final List<String> command = new ArrayList<>();

        command.add(JAVA);
        command.addAll(Arrays.asList(OPTIONS.split("\\s+")));
        command.add("-jar");
        command.add(JAR);

        COMMAND = Collections.unmodifiableList(command);
    }

    // endregion

    // region Methods

    @BeforeAll
    public static void validateJar() {
        assertThat(JAR).isNotBlank();
        assertThat(Paths.get(JAR)).exists();
    }

    /**
     * Starts the spring-boot-app and ensures that it completes with status code zero.
     */
    @Test
    public void withGlobalEndpoint() {

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> environment = builder.environment();

        environment.remove("AZURE_COSMOS_CASSANDRA_READ_DATACENTER");
        environment.remove("AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER");

        this.exec("global-endpoint", builder);
    }

    @Test
    public void withGlobalEndpointAndReadDatacenter() {

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> environment = builder.environment();
        environment.remove("AZURE_COSMOS_CASSANDRA_WRITE_DATACENTER");

        this.exec("global-endpoint-and-read-datacenter", builder);
    }

    @Test
    public void withReadDatacenterAndWriteDatacenter() {

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> environment = builder.environment();
        environment.remove("AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT");

        this.exec("read-datacenter-and-write-datacenter", builder);
    }

    // endregion

    // region Privates

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void exec(final String testCase, final ProcessBuilder processBuilder) {

        processBuilder.command(COMMAND)
            .redirectInput(ProcessBuilder.Redirect.INHERIT)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT);

        final Path path = Paths.get(System.getProperty("user.home"),
            ".local",
            "var",
            "log",
            "azure-cosmos-cassandra-spring-data.test.load-balancing-policy." + testCase + ".log");

        assertThatCode(() -> Files.deleteIfExists(path)).doesNotThrowAnyException();

        processBuilder.environment().put("AZURE_COSMOS_CASSANDRA_LOG_FILE", path.toString());

        final Process process;

        try {
            process = processBuilder.start();
        } catch (final Throwable error) {
            fail("failed to execute command '%s' due to %s", COMMAND, error);
            return;
        }

        try {
            assertThat(process.waitFor(TIMEOUT_IN_MINUTES, TimeUnit.MINUTES)).isTrue();
        } catch (final InterruptedException error) {
            fail("command '%s timed out after %d minutes", COMMAND, TIMEOUT_IN_MINUTES);
            return;
        }

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
