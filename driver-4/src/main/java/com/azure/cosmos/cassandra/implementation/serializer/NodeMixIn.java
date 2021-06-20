// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link Node} instances to JSON for use in log messages.
 */
@JsonAppend()
@JsonPropertyOrder(value = { "endPoint", "datacenter", "distance", "hostId", "schemaVersion" }, alphabetic = true)
@JsonIncludeProperties({
    "cassandraVersion",
    "datacenter",
    "distance",
    "endPoint",
    "hostId",
    "openConnections",
    "reconnecting",
    "schemaVersion",
    "state",
    "upSinceMillis" })
public abstract class NodeMixIn {
    public static final Class<Node> HANDLED_TYPE = Node.class;
}
