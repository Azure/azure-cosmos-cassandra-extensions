// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link MetricRegistry} instances to JSON for use in log messages.
 */
@JsonAppend()
@JsonPropertyOrder(alphabetic = true)
@JsonIgnoreProperties({ "metrics", "names" })
public abstract class MetricRegistryMixIn {
    public static final Class<MetricRegistry> HANDLED_TYPE = MetricRegistry.class;
}
