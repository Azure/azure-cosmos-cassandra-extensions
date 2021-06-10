// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Serializes {@link QueryTrace} instances to JSON for use in log messages.
 */
public final class QueryTraceSerializer extends StdSerializer<QueryTrace> {

    public static final QueryTraceSerializer INSTANCE = new QueryTraceSerializer();
    private static final long serialVersionUID = -990765956444594694L;

    private QueryTraceSerializer() {
        super(QueryTrace.class);
    }

    @Override
    public void serialize(
        @NonNull final QueryTrace value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();

        generator.writeObjectField("tracingId", value.getTracingId());
        provider.defaultSerializeField("coordinatorAddress", value.getCoordinatorAddress(), generator);
        provider.defaultSerializeField("requestType", value.getRequestType(), generator);
        provider.defaultSerializeField("parameters", value.getParameters(), generator);
        provider.defaultSerializeField("startedAt", value.getStartedAt(), generator);
        provider.defaultSerializeField("durationMicros", value.getDurationMicros(), generator);
        provider.defaultSerializeField("events", value.getEvents(), generator);

        generator.writeEndObject();
    }
}
