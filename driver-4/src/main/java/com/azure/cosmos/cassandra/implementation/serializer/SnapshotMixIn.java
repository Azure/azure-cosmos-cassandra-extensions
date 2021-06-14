// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link Snapshot} instances to JSON for use in log messages.
 */
@JsonAppend
@JsonIgnoreProperties({ "values" })
@JsonPropertyOrder(alphabetic = true)
public abstract class SnapshotMixIn {
    public static final Class<Snapshot> HANDLED_TYPE = Snapshot.class;
}
