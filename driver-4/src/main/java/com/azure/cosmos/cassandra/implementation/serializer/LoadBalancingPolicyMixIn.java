// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link LoadBalancingPolicy} instances to JSON for use in log messages.
 */
@JsonAppend(
    props = @JsonAppend.Prop(value = TypePropertyWriter.class, name = "type", type = String.class),
    prepend = true)
public abstract class LoadBalancingPolicyMixIn {
    public static final Class<LoadBalancingPolicy> HANDLED_TYPE = LoadBalancingPolicy.class;
}
