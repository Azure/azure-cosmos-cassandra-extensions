// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Serializes {@link Node} instances to JSON for use in log messages.
 */
public final class NodeSerializer extends StdSerializer<Node> {

    public static final NodeSerializer INSTANCE = new NodeSerializer();
    private static final long serialVersionUID = -4853942224745141238L;

    private NodeSerializer() {
        super(Node.class);
    }

    @Override
    public void serialize(
        @NonNull final Node value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();
        provider.defaultSerializeField("endPoint", value.getEndPoint(), generator);
        provider.defaultSerializeField("datacenter", value.getDatacenter(), generator);
        provider.defaultSerializeField("distance", value.getDistance(), generator);
        provider.defaultSerializeField("hostId", value.getHostId(), generator);
        provider.defaultSerializeField("openConnections", value.getOpenConnections(), generator);
        provider.defaultSerializeField("state", value.getState(), generator);
        generator.writeEndObject();
    }
}
