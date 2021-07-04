// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link DriverContext} instances to JSON for use in log messages.
 */
@JsonAppend()
@JsonPropertyOrder(value = { "sessionName", "protocolVersion", "startupOptions" }, alphabetic = true)
@JsonIncludeProperties({
    "authProvider",
    "loadBalancingPolicies",
    "protocolVersion",
    "reconnectionPolicy",
    "requestThrottler",
    "requestTracker",
    "retryPolicies",
    "sessionName",
    "speculativeExecutionPolicies",
    "startupOptions" })
public abstract class DriverContextMixIn {
    public static final Class<DriverContext> HANDLED_TYPE = DriverContext.class;
}
