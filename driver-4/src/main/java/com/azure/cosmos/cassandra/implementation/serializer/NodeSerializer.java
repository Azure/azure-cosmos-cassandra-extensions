// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Serializes a {@link Node} instance to JSON for use in log messages.
 */
public class NodeSerializer extends StdSerializer<Node> {

    public static final NodeSerializer INSTANCE = new NodeSerializer(Node.class);
    private static final long serialVersionUID = -4853942224745141238L;

    NodeSerializer(final Class<Node> type) {
        super(type);
    }

    @Override
    public void serialize(
        @NonNull final Node value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider serializerProvider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null serializerProvider");

        generator.writeStartObject();
        generator.writeStringField("endPoint", value.getEndPoint().toString());
        generator.writeStringField("datacenter", value.getDatacenter());
        generator.writeStringField("distance", value.getDistance().toString());

        final UUID hostId = value.getHostId();

        if (hostId == null) {
            generator.writeNullField("hostId");
        } else {
            generator.writeStringField("hostId", hostId.toString());
        }

        generator.writeNumberField("openConnections", value.getOpenConnections());
        generator.writeStringField("state", value.getState().toString());
        generator.writeEndObject();
    }
}
