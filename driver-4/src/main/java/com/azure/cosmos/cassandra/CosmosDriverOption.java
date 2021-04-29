// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Describes an option for a Cosmos Cassandra extension.
 * <p>
 * This class includes methods for getting and parsing option values, including default values. This interface is
 * implemented by {@link CosmosLoadBalancingPolicyOption} and {@link CosmosRetryPolicyOption}.
 */
public interface CosmosDriverOption extends DriverOption {

    /**
     * Returns the name this {@link CosmosDriverOption Cosmos driver option}.
     *
     * @return The name of this {@link CosmosDriverOption Cosmos driver option}.
     */
    @NonNull
    String getName();

    /**
     * Returns the default value of this {@link CosmosDriverOption Cosmos driver option}.
     *
     * @param type The class of values for this {@link CosmosDriverOption Cosmos driver option}.
     * @param <T>  The class of values for this {@link CosmosDriverOption Cosmos driver option}.
     *
     * @return The default value for this {@link CosmosDriverOption Cosmos driver option}.
     */
    @NonNull
    <T> T getDefaultValue(@NonNull Class<T> type);

    /**
     * Gets the value for this {@link CosmosDriverOption Cosmos driver option} from the given {@link
     * DriverExecutionProfile driver execution profile}.
     *
     * @param profile A {@link DriverExecutionProfile driver execution profile} from which to get the value.
     * @param type    The class of values for this {@link CosmosDriverOption Cosmos driver option}.
     * @param <T>     The class of values for this {@link CosmosDriverOption Cosmos driver option}.
     *
     * @return The default value for this {@link CosmosDriverOption Cosmos driver option}.
     */
    @NonNull
    <T> T getValue(@NonNull DriverExecutionProfile profile, @NonNull Class<T> type);

    /**
     * Parses a value for this {@link CosmosDriverOption Cosmos driver option}.
     *
     * @param value   A {@link String string} value to parse.
     * @param type    The class of values for this {@link CosmosDriverOption Cosmos driver option}.
     * @param <T>     The class of values for this {@link CosmosDriverOption Cosmos driver option}.
     *
     * @return The default value for this {@link CosmosDriverOption Cosmos driver option}.
     */
    @NonNull
    <T> T parse(@Nullable String value, @NonNull Class<T> type);
}
