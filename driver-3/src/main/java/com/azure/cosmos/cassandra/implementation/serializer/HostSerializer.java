// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.VersionNumber;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

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
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        final VersionNumber cassandraVersion = value.getCassandraVersion();

        generator.writeStartObject();

        provider.defaultSerializeField("endPoint", value.getEndPoint().toString(), generator);
        provider.defaultSerializeField("datacenter", value.getDatacenter(), generator);
        provider.defaultSerializeField("hostId", value.getHostId(), generator);
        provider.defaultSerializeField("state", value.getState(), generator);
        provider.defaultSerializeField("schemaVersion", value.getSchemaVersion(), generator);

        provider.defaultSerializeField("cassandraVersion", cassandraVersion == null
                ? null
                : cassandraVersion.toString(),
            generator);

        generator.writeEndObject();
    }
}
