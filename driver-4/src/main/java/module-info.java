// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

module com.azure.cosmos.cassandra {

    exports com.azure.cosmos.cassandra;
    exports com.azure.cosmos.cassandra.implementation to com.azure.cosmos.cassandra.config;
    exports com.azure.cosmos.cassandra.implementation.serializer to com.fasterxml.jackson.databind;

    // Named modules

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jdk8;

    // Automatic modules

    requires com.datastax.oss.driver.core;
    requires com.codahale.metrics;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
}
