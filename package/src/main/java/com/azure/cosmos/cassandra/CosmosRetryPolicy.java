// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Objects;
import java.util.Random;
import java.util.function.BiFunction;

/**
 * Implements a Cassandra {@link RetryPolicy} with back-offs for {@link OverloadedException} failures.
 * <p>
 * {@link #maxRetryCount} specifies the number of retries that should be attempted. A value of -1 specifies that an
 * indefinite number of retries should be attempted. For {@link #onReadTimeout}, {@link #onWriteTimeout}, and {@link
 * #onUnavailable}, we retry immediately. For onRequestErrors such as OverLoadedError, we try to parse the exception
 * message and use RetryAfterMs field provided from the server as the back-off duration. If RetryAfterMs is not
 * available, we default to exponential growing back-off scheme. In this case the time between retries is increased by
 * {@link #growingBackoffTimeInMillis} milliseconds (default: 1000 ms) on each retry, unless maxRetryCount is -1, in
 * which case we back-off with fixed {@link #fixedBackoffTimeInMillis} duration.
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final int GROWING_BACKOFF_SALT_IN_MILLIS = 2_000;
    private static final Random RANDOM = new Random();

    private final int fixedBackoffTimeInMillis;
    private final int growingBackoffTimeInMillis;
    private final int maxRetryCount;

    // endregion

    // region Constructors

    public CosmosRetryPolicy(final DriverContext driverContext, final String profileName) {

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.maxRetryCount = Option.MAX_RETRIES.getValue(profile);
        this.fixedBackoffTimeInMillis = Option.FIXED_BACKOFF_TIME.getValue(profile);
        this.growingBackoffTimeInMillis = Option.GROWING_BACKOFF_TIME.getValue(profile);
    }

    CosmosRetryPolicy(final int maxRetryCount) {
        this(maxRetryCount, Option.FIXED_BACKOFF_TIME.getDefaultValue(), Option.GROWING_BACKOFF_TIME.getDefaultValue());
    }

    CosmosRetryPolicy(final int maxRetryCount, final int fixedBackOffTimeInMillis, final int growingBackoffTimeInMillis) {
        this.maxRetryCount = maxRetryCount;
        this.fixedBackoffTimeInMillis = fixedBackOffTimeInMillis;
        this.growingBackoffTimeInMillis = growingBackoffTimeInMillis;
    }

    // endregion

    // region Accessors

    public int getFixedBackoffTimeInMillis() {
        return this.fixedBackoffTimeInMillis;
    }

    public int getGrowingBackoffTimeInMillis() {
        return this.growingBackoffTimeInMillis;
    }

    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    // endregion

    // region Methods

    @Override
    public void close() {
    }

    @Override
    public RetryDecision onErrorResponse(
        @NonNull final Request request,
        @NonNull final CoordinatorException error,
        final int retryCount) {

        RetryDecision retryDecision;

        // TODO (DANOBLE) ServerError is not the same as ConnectionException and there is no obvious replacement
        //  Consequently this decision tree must be rethought based on the new and very different CoordinatorException
        //  hierarchy
        try {
            if (error instanceof ServerError) {
                return this.retryManyTimesOrThrow(retryCount);
            }
            if (error instanceof OverloadedException || error instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                    int retryAfterMillis = getRetryAfterMillis(error.toString());
                    if (retryAfterMillis == -1) {
                        retryAfterMillis = this.maxRetryCount == -1
                            ? this.fixedBackoffTimeInMillis
                            : this.growingBackoffTimeInMillis * retryCount + RANDOM.nextInt(GROWING_BACKOFF_SALT_IN_MILLIS);
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

        return this.retryManyTimesOrThrow(retryCount);
    }

    @Override
    public RetryDecision onRequestAborted(@NonNull final Request request, @NonNull final Throwable error, final int retryCount) {
        // TODO (DANOBLE) Implement CosmosRetryPolicy.onRequestAborted
        return null;
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

        return this.retryManyTimesOrThrow(retryCount);
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
            if (kvp[0].trim().equals("RetryAfterMs")) {
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

    private final static String PATH_PREFIX = DefaultDriverOption.RETRY_POLICY.getPath() + ".";

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
        private final String path;

        <T, R> Option(final String name, final BiFunction<Option, DriverExecutionProfile, R> getter, final T defaultValue) {
            this.defaultValue = defaultValue;
            this.getter = getter;
            this.path = PATH_PREFIX + name;
        }

        @SuppressWarnings("unchecked")
        public <T> T getDefaultValue() {
            return (T) this.defaultValue;
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
