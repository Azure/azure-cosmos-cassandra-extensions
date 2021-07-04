// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Metadata;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A {@link JsonSerializer} for serializing a {@link Metadata} object into JSON.
 */
public final class MetadataSerializer extends StdSerializer<Metadata> {

    public static final MetadataSerializer INSTANCE = new MetadataSerializer();
    private static final long serialVersionUID = 2234571397397316058L;

    private MetadataSerializer() {
        super(Metadata.class);
    }

    @Override
    public void serialize(
        @NonNull final Metadata value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();
        provider.defaultSerializeField("clusterName", value.getClusterName(), generator);
        provider.defaultSerializeField("hosts", value.getAllHosts(), generator);
        provider.defaultSerializeField("keyspaces", value.getKeyspaces(), generator);
        generator.writeEndObject();
    }
}
