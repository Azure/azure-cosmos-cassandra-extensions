// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes the set of Cosmos retry policy options.
 */
public enum CosmosRetryPolicyOption implements CosmosDriverOption {

    /**
     * Fixed back-off time for delayed retries triggered by {@link CosmosRetryPolicy#onErrorResponse}.
     * <p>
     * Delays are triggered in response to {@link OverloadedException} errors. The delay time is extracted from the
     * error message, or--if the delay time isn't present in the error message--computed as follows:
     * <p><pre>{@code
     *   this.maxRetryCount == -1
     *     ? fixedBackOffTimeInMillis
     *     : retryCount * growingBackOffTimeInMillis + saltValue;
     * }</pre></p>
     */
    FIXED_BACKOFF_TIME("fixed-backoff-time",
        (option, profile) -> profile.getInt(option, option.getDefaultValue(Integer.class)),
        Integer::parseUnsignedInt,
        5_000),

    /**
     * Growing back-off time for delayed retries triggered by {@link CosmosRetryPolicy#onErrorResponse}.
     * <p>
     * Delays are triggered in response to {@link OverloadedException} errors. The delay time is extracted from the
     * error message, or--if the delay time isn't present in the error message--computed as follows:
     * <p><pre>{@code
     *   this.maxRetryCount == -1
     *     ? fixedBackOffTimeInMillis
     *     : retryCount * growingBackOffTimeInMillis + saltValue;
     * }</pre></p>
     */
    GROWING_BACKOFF_TIME("growing-backoff-time",
        (option, profile) -> profile.getInt(option, option.getDefaultValue(Integer.class)),
        Integer::parseUnsignedInt,
        1_000),

    /**
     * Maximum number of retries to attempt.
     * <p>
     * A value of {@code -1} indicates that an indefinite number of retries should be attempted.
     */
    MAX_RETRIES("max-retries",
        (option, profile) -> profile.getInt(option, option.getDefaultValue(Integer.class)),
        Integer::parseUnsignedInt,
        5),

    /**
     * A value indicating whether read timeout retries are enabled.
     * <p>
     * Disabling read timeout retries may be desirable when Cosmos DB server-side retries are enabled.
     */
    READ_TIMEOUT_RETRIES("read-timeout-retries",
        (option, profile) -> profile.getBoolean(option, option.getDefaultValue(Boolean.class)),
        Boolean::parseBoolean,
        true
    ),

    /**
     * A value indicating whether write timeout retries are enabled.
     * <p>
     * Disabling write timeout retries may be desirable when Cosmos DB server-side retries are enabled.
     */
    WRITE_TIMEOUT_RETRIES("write-timeout-retries",
        (option, profile) -> profile.getBoolean(option, option.getDefaultValue(Boolean.class)),
        Boolean::parseBoolean,
        true
    );

    // region Fields

    private final transient BiFunction<CosmosRetryPolicyOption, DriverExecutionProfile, ?> getter;
    private final transient Function<String, ?> parser;
    private final Object defaultValue;
    private final String name;
    private final String path;

    // endregion

    // region Constructors

    <T> CosmosRetryPolicyOption(
        final String name,
        final BiFunction<CosmosRetryPolicyOption, DriverExecutionProfile, T> getter,
        final Function<String, T> parser,
        final T defaultValue) {

        this.defaultValue = defaultValue;
        this.getter = getter;
        this.parser = parser;
        this.name = name;
        this.path = DefaultDriverOption.RETRY_POLICY.getPath() + '.' + name;
    }

    // endregion

    // region Methods

    @Override
    @NonNull
    public <T> T getDefaultValue(@NonNull final Class<T> type) {
        Objects.requireNonNull(type, "expected non-null type");
        return type.cast(this.defaultValue);
    }

    @Override
    @NonNull
    public String getName() {
        return this.name;
    }

    @Override
    @NonNull
    public String getPath() {
        return this.path;
    }

    @Override
    @NonNull
    public <T> T getValue(@NonNull final DriverExecutionProfile profile, @NonNull final Class<T> type) {
        Objects.requireNonNull(profile, "expected non-null profile");
        Objects.requireNonNull(type, "expected non-null type");
        return type.cast(this.getter.apply(this, profile));
    }

    @Override
    @NonNull
    public <T> T parse(@Nullable final String value, @NonNull final Class<T> type) {
        Objects.requireNonNull(type, "expected non-null type");
        return type.cast(value == null ? this.defaultValue : this.parser.apply(value));
    }

    // endregion
}
