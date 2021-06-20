// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.time.Duration;
import java.time.Instant;

final class JsonRegistrar {

    JsonRegistrar() {
    }

    void registerSerializers() {
        Json.objectMapper()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .registerModule(new Jdk8Module());
        Json.addSerializer(Duration.class, ToStringSerializer.instance)
            .addSerializer(EndPoint.class, ToStringSerializer.instance)
            .addSerializer(Instant.class, ToStringSerializer.instance)
            .addSerializer(StackTraceElement.class, ToStringSerializer.instance)
            .addSerializer(Version.class, ToStringSerializer.instance);
        Json.addSerializersAndMixIns(JsonRegistrar.class.getPackage().getName() + ".serializer");
    }
}
