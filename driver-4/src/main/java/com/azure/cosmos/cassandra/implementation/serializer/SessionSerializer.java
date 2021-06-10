// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.session.Session;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Serializes {@link Session} instances to JSON for use in log messages.
 */
public final class SessionSerializer extends StdSerializer<Session> {

    public static final SessionSerializer INSTANCE = new SessionSerializer();
    private static final long serialVersionUID = -5623039502602836915L;

    private SessionSerializer() {
        super(Session.class);
    }

    @Override
    public void serialize(
        @NonNull final Session value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();

        provider.defaultSerializeField("name", value.getName(), generator);
        provider.defaultSerializeField("keyspace", value.getKeyspace(), generator);
        provider.defaultSerializeField("context", value.getContext(), generator);
        provider.defaultSerializeField("metadata", value.getMetadata(), generator);
        provider.defaultSerializeField("metrics", value.getMetrics(), generator);

        generator.writeEndObject();
    }
}
