// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes the set of Cosmos load balancing policy options.
 */
public enum CosmosLoadBalancingPolicyOption implements CosmosDriverOption {

    DNS_EXPIRY_TIME("dns-expiry-time",
        (option, profile) -> profile.getInt(option, option.getDefaultValue(Integer.class)),
        Integer::parseUnsignedInt,
        60),

    GLOBAL_ENDPOINT("global-endpoint",
        (option, profile) -> {
            final String value = profile.getString(option, option.getDefaultValue(String.class));
            assert value != null;
            final int index = value.lastIndexOf(':');
            return index < 0 ? value : value.substring(0, index);
        },
        Function.identity(),
        ""),

    READ_DATACENTER("read-datacenter",
        (option, profile) -> profile.getString(option, option.getDefaultValue(String.class)),
        Function.identity(),
        ""),

    WRITE_DATACENTER("write-datacenter",
        (option, profile) -> profile.getString(option, option.getDefaultValue(String.class)),
        Function.identity(),
        "");

    private final Object defaultValue;
    private final BiFunction<CosmosLoadBalancingPolicyOption, DriverExecutionProfile, ?> getter;
    private final Function<String, ?> parser;
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
