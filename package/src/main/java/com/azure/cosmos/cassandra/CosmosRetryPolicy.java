// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;
import java.util.function.BiFunction;

/**
 * A {@link RetryPolicy} implementation with back-offs for {@link CoordinatorException} failures.
 * <p>
 * This is the default retry policy when you take a dependency on this package. It provides a good out-of-box
 * experience for communicating with Cosmos Cassandra API instances. Its behavior is specified in configuration:
 * <pre>{@code
 * datastax-java-driver.advanced.retry-policy {
 *   class = DefaultRetryPolicy
 *   max-retries = 5              # Maximum number of retries
 *   fixed-backoff-time = 5000    # Fixed backoff time in milliseconds
 *   growing-backoff-time = 1000  # Growing backoff time in milliseconds
 * }
 * }</pre>
 * The number of retries that should be attempted is specified by {@code max-retries}. A value of {@code -1} specifies
 * that an indefinite number of retries should be attempted. For {@link #onReadTimeout onReadTimout},
 * {@link #onWriteTimeout onWriteTimeout}, and {@link #onUnavailable onUnavailable}, retries are immediate. For
 * {@link #onErrorResponse onResponse}, the exception message is parsed to obtain the value of the {@code RetryAfterMs}
 * field provided by the server as the back-off duration. If {@code RetryAfterMs} is not available, the default
 * exponential growing back-off scheme is used. In this case the time between retries is increased by
 * {@code growing-backoff-time} on each retry, unless {@code max-retries} is {@code -1}. In this case back-off occurs
 * with fixed {@code fixed-backoff-time} duration.
 *
 * @see <a href="../../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries/">DataStax Java
 * Driver Retries</a>
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final int GROWING_BACKOFF_SALT_IN_MILLIS = 2_000;
    private static final Logger LOG = LoggerFactory.getLogger(CosmosRetryPolicy.class);
    private static final String PATH_PREFIX = DefaultDriverOption.RETRY_POLICY.getPath() + ".";
    private static final Random RANDOM = new Random();

    private final int fixedBackoffTimeInMillis;
    private final int growingBackoffTimeInMillis;
    private final int maxRetryCount;

    // endregion

    // region Constructors

    /**
     * Initializes a newly created {@link CosmosRetryPolicy} object from configuration.
     *
     * @param driverContext an object holding the context of the current driver instance.
     * @param profileName   name of the configuration profile to apply.
     */
    @SuppressWarnings("unused")
    public CosmosRetryPolicy(final DriverContext driverContext, final String profileName) {

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.maxRetryCount = Option.MAX_RETRIES.getValue(profile);
        this.fixedBackoffTimeInMillis = Option.FIXED_BACKOFF_TIME.getValue(profile);
        this.growingBackoffTimeInMillis = Option.GROWING_BACKOFF_TIME.getValue(profile);
    }

    CosmosRetryPolicy(final int maxRetryCount) {
        this(maxRetryCount, Option.FIXED_BACKOFF_TIME.getDefaultValue(), Option.GROWING_BACKOFF_TIME.getDefaultValue());
    }

    CosmosRetryPolicy(
        final int maxRetryCount, final int fixedBackOffTimeInMillis, final int growingBackoffTimeInMillis) {

        this.maxRetryCount = maxRetryCount;
        this.fixedBackoffTimeInMillis = fixedBackOffTimeInMillis;
        this.growingBackoffTimeInMillis = growingBackoffTimeInMillis;
    }

    // endregion

    // region Accessors

    /**
     * Gets the {@code fixed-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     *
     * @return the {@code fixed-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     */
    public int getFixedBackoffTimeInMillis() {
        return this.fixedBackoffTimeInMillis;
    }

    /**
     * Gets the {@code growing-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     *
     * @return the {@code growing-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     */
    public int getGrowingBackoffTimeInMillis() {
        return this.growingBackoffTimeInMillis;
    }

    /**
     * Gets the {@code max-retries} specified by this {@link CosmosRetryPolicy} object.
     *
     * @return the {@code max-retries} specified by this {@link CosmosRetryPolicy} object.
     */
    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    /**
     * Closes the current {@link CosmosRetryPolicy}.
     */
    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public RetryDecision onErrorResponse(
        @NonNull final Request request, @NonNull final CoordinatorException error, final int retryCount) {

        RetryDecision retryDecision;

        try {
            if (error instanceof OverloadedException || error instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                    int retryAfterMillis = getRetryAfterMillis(error.toString());
                    if (retryAfterMillis == -1) {
                        retryAfterMillis = this.maxRetryCount == -1
                            ? this.fixedBackoffTimeInMillis
                            : this.growingBackoffTimeInMillis * retryCount + RANDOM.nextInt(
                                GROWING_BACKOFF_SALT_IN_MILLIS);
                    }
                    Thread.sleep(retryAfterMillis);
                    retryDecision = RetryDecision.RETRY_SAME;
                } else {
                    retryDecision = RetryDecision.RETHROW;
                }
            } else {
                retryDecision = RetryDecision.RETHROW;
            }

        } catch (final InterruptedException exception) {
            retryDecision = RetryDecision.RETHROW;
        }

        return retryDecision;
    }

    @Override
    public RetryDecision onReadTimeout(
        @NonNull final Request request,
        @NonNull final ConsistencyLevel consistencyLevel,
        final int blockFor,
        final int received,
        final boolean dataPresent,
        final int retryCount) {

        final RetryDecision decision = retryCount == 0 && received >= blockFor && !dataPresent
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_SAME && LOG.isDebugEnabled()) {
            LOG.debug(
                "retrying on read timeout on same host: { "
                    + "consistencyLevel: {}, "
                    + "blockFor: {}, "
                    + "dataPresent: {}, "
                    + "received: {}, "
                    + "retryCount: {} }",
                consistencyLevel,
                blockFor,
                false,
                received,
                retryCount);
        }

        return decision;
    }

    @Override
    public RetryDecision onRequestAborted(
        @NonNull final Request request, @NonNull final Throwable error, final int retryCount) {

        return error instanceof ClosedConnectionException || error instanceof HeartbeatException
            ? this.retryManyTimesOrThrow(retryCount)
            : null;
    }

    @Override
    public RetryDecision onUnavailable(
        @NonNull final Request request,
        @NonNull final ConsistencyLevel consistencyLevel,
        final int required,
        final int alive,
        final int retryCount) {

        return this.retryManyTimesOrThrow(retryCount);
    }

    @Override
    public RetryDecision onWriteTimeout(
        @NonNull final Request request,
        @NonNull final ConsistencyLevel consistencyLevel,
        @NonNull final WriteType writeType,
        final int blockFor,
        final int received,
        final int retryCount) {

        final RetryDecision decision = retryCount == 0 && writeType == DefaultWriteType.BATCH_LOG
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_SAME && LOG.isDebugEnabled()) {
            LOG.debug(
                "retrying on write timeout on same host: { "
                    + "consistencyLevel: {}, "
                    + "writeType: {}, "
                    + "blockFor: {}, "
                    + "received: {}, "
                    + "retryCount: {} }",
                consistencyLevel,
                writeType,
                blockFor,
                received,
                retryCount);
        }
        return decision;
    }

    @Override
    public String toString() {
        return "CosmosRetryPolicy({"
            + Option.FIXED_BACKOFF_TIME.getName() + ':' + this.fixedBackoffTimeInMillis + ','
            + Option.GROWING_BACKOFF_TIME.getName() + ':' + this.growingBackoffTimeInMillis + ','
            + Option.MAX_RETRIES.getName() + ':' + this.maxRetryCount + "})";
    }

    // endregion

    // region Privates

    // Example exceptionString:
    // "com.datastax.driver.core.exceptions.OverloadedException: Queried host (babas.cassandra.cosmos.azure.com/40
    // .65.106.154:10350)
    // was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10,
    // Additional details='
    private static int getRetryAfterMillis(final String exceptionString) {

        final String[] tokens = exceptionString.split(",");

        for (final String token : tokens) {

            final String[] kvp = token.split("=");

            if (kvp.length != 2) {
                continue;
            }
            if ("RetryAfterMs".equals(kvp[0].trim())) {
                return Integer.parseInt(kvp[1]);
            }
        }

        return -1;
    }

    private RetryDecision retryManyTimesOrThrow(final int retryCount) {
        return this.maxRetryCount == -1 || retryCount < this.maxRetryCount
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;
    }

    // endregion

    // region Types

    enum Option implements DriverOption {

        FIXED_BACKOFF_TIME("fixed-backoff-time", (option, profile) ->
            profile.getInt(option, option.getDefaultValue()),
            5_000),

        GROWING_BACKOFF_TIME("growing-backoff-time", (option, profile) ->
            profile.getInt(option, option.getDefaultValue()),
            1_000),

        MAX_RETRIES("max-retries", (option, profile) ->
            profile.getInt(option, option.getDefaultValue()),
            5);

        private final Object defaultValue;
        private final BiFunction<Option, DriverExecutionProfile, ?> getter;
        private final String name;
        private final String path;

        <T, R> Option(
            final String name, final BiFunction<Option, DriverExecutionProfile, R> getter, final T defaultValue) {

            this.defaultValue = defaultValue;
            this.getter = getter;
            this.name = name;
            this.path = PATH_PREFIX + name;
        }

        @SuppressWarnings("unchecked")
        public <T> T getDefaultValue() {
            return (T) this.defaultValue;
        }

        @NonNull
        public String getName() {
            return this.name;
        }

        @NonNull
        @Override
        public String getPath() {
            return this.path;
        }

        @SuppressWarnings("unchecked")
        public <T> T getValue(@NonNull final DriverExecutionProfile profile) {
            Objects.requireNonNull(profile, "expected non-null profile");
            return (T) this.getter.apply(this, profile);
        }
    }

    // endregion
}
