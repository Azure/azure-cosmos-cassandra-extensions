// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link ReconnectionPolicy} instances to JSON for use in log messages.
 */
@JsonAppend(
    props = @JsonAppend.Prop(value = TypePropertyWriter.class, name = "type", type = String.class),
    prepend = true)
public abstract class ReconnectionPolicyMixIn {
    public static final Class<ReconnectionPolicy> HANDLED_TYPE = ReconnectionPolicy.class;
}
