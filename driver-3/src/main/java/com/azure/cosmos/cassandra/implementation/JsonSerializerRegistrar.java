// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.datastax.driver.core.EndPoint;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import java.time.Duration;
import java.time.Instant;

final class JsonSerializerRegistrar {

    private JsonSerializerRegistrar() {
        throw new UnsupportedOperationException();
    }

    static void init() {
        Json.module()
            .addSerializer(EndPoint.class, ToStringSerializer.instance)
            .addSerializer(Duration.class, ToStringSerializer.instance)
            .addSerializer(Instant.class, ToStringSerializer.instance)
            .addSerializer(StackTraceElement.class, ToStringSerializer.instance);
        Json.registerSerializers(JsonSerializerRegistrar.class.getPackage().getName() + ".serializer");
    }
}
