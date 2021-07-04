// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metrics;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A {@link JsonSerializer} for serializing a {@link Cluster} object into JSON.
 */
public final class ClusterSerializer extends StdSerializer<Cluster> {

    public static final ClusterSerializer INSTANCE = new ClusterSerializer();
    private static final long serialVersionUID = -2084259636087581282L;

    private ClusterSerializer() {
        super(Cluster.class);
    }

    @Override
    public void serialize(
        @NonNull final Cluster value,
        @NonNull final JsonGenerator generator,
        @NonNull final SerializerProvider provider) throws IOException {

        requireNonNull(value, "expected non-null value");
        requireNonNull(value, "expected non-null generator");
        requireNonNull(value, "expected non-null provider");

        generator.writeStartObject();
        provider.defaultSerializeField("clusterName", value.getClusterName(), generator);
        provider.defaultSerializeField("closed", value.isClosed(), generator);

        Metrics metrics = null;

        if (value.isClosed()) {
            generator.writeNullField("metadata");
        } else {
            try {
                // Side-effect: Cluster::getMetadata calls Metadata::init which throws, if the call fails
                provider.defaultSerializeField("metadata", value.getMetadata(), generator);
                metrics = value.getMetrics();
            } catch (final Throwable error) {
                provider.defaultSerializeField("metadata", error, generator);
            }
        }

        provider.defaultSerializeField("metrics", metrics, generator);
        generator.writeEndObject();
    }
}
