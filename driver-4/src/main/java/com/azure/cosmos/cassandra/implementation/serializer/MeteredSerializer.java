// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.codahale.metrics.Metered;
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
public final class MeteredSerializer extends StdSerializer<Metered> {

    public static final MeteredSerializer INSTANCE = new MeteredSerializer();
    private static final long serialVersionUID = -8621187766912377954L;

    private MeteredSerializer() {
        super(Metered.class);
    }

    @Override
    public void serialize(
        @NonNull final Metered value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();
        provider.defaultSerializeField("meanRate", value.getMeanRate(), generator);
        provider.defaultSerializeField("count", value.getCount(), generator);
        provider.defaultSerializeField("oneMinuteRate", value.getOneMinuteRate(), generator);
        provider.defaultSerializeField("fiveMinuteRate", value.getFiveMinuteRate(), generator);
        provider.defaultSerializeField("fifteenMinuteRate", value.getFifteenMinuteRate(), generator);
        generator.writeEndObject();
    }
}
