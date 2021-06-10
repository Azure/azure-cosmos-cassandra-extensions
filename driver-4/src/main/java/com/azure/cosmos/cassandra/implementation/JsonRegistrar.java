// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import java.time.Duration;
import java.time.Instant;

final class JsonRegistrar {

    JsonRegistrar() {
    }

    void registerSerializers() {
        Json.objectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        Json.addSerializer(Duration.class, ToStringSerializer.instance)
            .addSerializer(Instant.class, ToStringSerializer.instance)
            .addSerializer(StackTraceElement.class, ToStringSerializer.instance);
        Json.addSerializersAndMixIns(JsonRegistrar.class.getPackage().getName() + ".serializer");
    }
}
