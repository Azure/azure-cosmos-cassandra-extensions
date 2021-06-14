// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.codahale.metrics.Sampling;
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
public final class SamplingSerializer extends StdSerializer<Sampling> {

    public static final SamplingSerializer INSTANCE = new SamplingSerializer();
    private static final long serialVersionUID = 4593768855567990092L;

    private SamplingSerializer() {
        super(Sampling.class);
    }

    @Override
    public void serialize(
        @NonNull final Sampling value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        provider.defaultSerializeValue(value.getSnapshot(), generator);
    }
}
