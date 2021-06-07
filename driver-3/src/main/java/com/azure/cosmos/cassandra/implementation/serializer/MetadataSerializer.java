// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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
        @NonNull final SerializerProvider serializerProvider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null serializerProvider");

        generator.writeStartObject();

        final String clusterName = value.getClusterName();

        if (clusterName == null) {
            generator.writeNullField("clusterName");
        } else {
            generator.writeStringField("clusterName", clusterName);
        }

        final Set<Host> hosts = value.getAllHosts();

        if (hosts == null) {
            generator.writeNullField("hosts");
        } else {
            generator.writeArrayFieldStart("keyspaces");
            for (final Host host : hosts) {
                generator.writeObject(host);
            }
        }

        final List<KeyspaceMetadata> keyspacesMetadata = value.getKeyspaces();

        if (keyspacesMetadata == null) {
            generator.writeNullField("keyspaces");
        } else {
            generator.writeArrayFieldStart("keyspaces");
            for (final KeyspaceMetadata keyspaceMetadata : keyspacesMetadata) {
                generator.writeString(keyspaceMetadata.toString());
            }
        }

        generator.writeEndObject();
    }
}
