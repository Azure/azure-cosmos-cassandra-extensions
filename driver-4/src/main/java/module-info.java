// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

module com.azure.cosmos.cassandra {

    exports com.azure.cosmos.cassandra;
    exports com.azure.cosmos.cassandra.implementation to com.azure.cosmos.cassandra.config;

    // Named modules

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;

    // Automatic modules

    requires transitive com.datastax.oss.driver.core;
    requires com.codahale.metrics;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
}
