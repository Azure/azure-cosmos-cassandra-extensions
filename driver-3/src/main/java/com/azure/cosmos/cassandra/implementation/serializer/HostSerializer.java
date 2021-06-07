// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Host;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * A {@link JsonSerializer} for serializing a {@link Host} object into JSON.
 */
public final class HostSerializer extends StdSerializer<Host> {

    public static final HostSerializer INSTANCE = new HostSerializer();
    private static final long serialVersionUID = -7559845199616549188L;

    private HostSerializer() {
        super(Host.class);
    }

    @Override
    public void serialize(
        @NonNull final Host value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider serializerProvider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null serializerProvider");

        generator.writeStartObject();
        generator.writeStringField("endPoint", value.getEndPoint().toString());
        generator.writeStringField("datacenter", value.getDatacenter());

        final UUID hostId = value.getHostId();

        if (hostId == null) {
            generator.writeNullField("hostId");
        } else {
            generator.writeStringField("hostId", hostId.toString());
        }

        generator.writeStringField("state", value.getState());
        generator.writeEndObject();
    }
}
