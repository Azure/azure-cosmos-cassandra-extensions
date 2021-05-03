// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes the set of Cosmos load balancing policy options.
 */
public enum CosmosLoadBalancingPolicyOption implements CosmosDriverOption {

    MULTI_REGION_WRITES("multi-region-writes",
        (option, profile) -> profile.getBoolean(option, option.getDefaultValue(Boolean.class)),
        Boolean::parseBoolean,
        false),

    @SuppressWarnings("unchecked")
    PREFERRED_REGIONS("preferred-regions",
        (CosmosLoadBalancingPolicyOption option, DriverExecutionProfile profile) -> {
            final List<String> value = profile.getStringList(option, option.getDefaultValue(List.class));
            assert value != null;
            return value;
        },
        value -> value,
        Collections.emptyList());

    private final Object defaultValue;
    private final transient BiFunction<CosmosLoadBalancingPolicyOption, DriverExecutionProfile, ?> getter;
    private final transient Function<String, ?> parser;
    private final String name;
    private final String path;

    <T> CosmosLoadBalancingPolicyOption(
        final String name,
        final BiFunction<CosmosLoadBalancingPolicyOption, DriverExecutionProfile, T> getter,
        final Function<String, T> parser,
        final T defaultValue) {

        this.defaultValue = defaultValue;
        this.getter = getter;
        this.parser = parser;
        this.name = name;
        this.path = DefaultDriverOption.LOAD_BALANCING_POLICY.getPath() + '.' + name;
    }

    @Override
    @NonNull
    public <T> T getDefaultValue(@NonNull final Class<T> type) {
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
        return type.cast(this.getter.apply(this, profile));
    }

    @Override
    @NonNull
    public <T> T parse(@Nullable final String value, @NonNull final Class<T> type) {
        return type.cast(value == null ? this.defaultValue : this.parser.apply(value));
    }
}
