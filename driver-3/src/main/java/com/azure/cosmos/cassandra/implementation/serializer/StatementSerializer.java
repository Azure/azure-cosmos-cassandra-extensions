// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.util.Objects.requireNonNull;

/**
 * A {@link JsonSerializer} for serializing a {@link Statement} object into JSON.
 */
public final class StatementSerializer extends StdSerializer<Statement> {

    public static final StatementSerializer INSTANCE = new StatementSerializer();

    private static final long serialVersionUID = 3572602626172107511L;

    private StatementSerializer() {
        super(Statement.class);
    }

    @Override
    public void serialize(
        @NonNull final Statement value,
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

        final String keyspace = value.getKeyspace();

        if (keyspace == null) {
            generator.writeNullField("keyspace");
        } else {
            generator.writeStringField("keyspace", keyspace);
        }

        generator.writeEndObject();
    }
}
