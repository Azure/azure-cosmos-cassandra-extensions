// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.session.Request;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Serializes a {@link Request} instance to JSON for use in log messages.
 */
public class RequestSerializer extends StdSerializer<Request> {

    public static final RequestSerializer INSTANCE = new RequestSerializer(Request.class);
    private static final long serialVersionUID = -6046496854932624633L;

    RequestSerializer(final Class<Request> type) {
        super(type);
    }

    @Override
    public void serialize(
        @NonNull final Request value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider serializerProvider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null serializerProvider");

        generator.writeStartObject();

        String query;

        try {
            final Method getQuery = value.getClass().getMethod("getQuery");
            query = (String) getQuery.invoke(value);
        } catch (final NoSuchMethodException | InvocationTargetException | IllegalAccessException error) {
            query = value.getClass().toString();
        }

        if (query == null) {
            generator.writeNullField("query");
        } else {
            generator.writeStringField("query", query);
        }

        final CqlIdentifier keyspace = value.getKeyspace();

        if (keyspace == null) {
            generator.writeNullField("keyspace");
        } else {
            generator.writeStringField("keyspace", keyspace.asCql(true));
        }

        final Duration timeout = value.getTimeout();

        if (timeout == null) {
            generator.writeNullField("timeout");
        } else {
            generator.writeObjectField("timeout", timeout);
        }

        generator.writeEndObject();
    }
}
