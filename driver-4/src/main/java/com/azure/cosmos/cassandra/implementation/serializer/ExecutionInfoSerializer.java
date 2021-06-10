// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Serializes {@link ExecutionInfo} instances to JSON for use in log messages.
 */
public final class ExecutionInfoSerializer extends StdSerializer<ExecutionInfo> {

    public static final ExecutionInfoSerializer INSTANCE = new ExecutionInfoSerializer();
    private static final long serialVersionUID = 6771267285211657125L;

    private ExecutionInfoSerializer() {
        super(ExecutionInfo.class);
    }

    @Override
    public void serialize(
        @NonNull final ExecutionInfo value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();

        provider.defaultSerializeField("request", value.getRequest(), generator);
        provider.defaultSerializeField("errors", value.getErrors(), generator);
        provider.defaultSerializeField("warnings", value.getWarnings(), generator);

        provider.defaultSerializeField("queryTrace", value.getTracingId() == null
            ? null
            : value.getQueryTrace(), generator);

        generator.writeEndObject();
    }
}
