// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

module com.azure.cosmos.cassandra.config {

    exports com.azure.cosmos.cassandra.config;

    // Named modules

    requires com.azure.cosmos.cassandra;

    // Automatic modules

    requires com.datastax.oss.driver.core;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
    requires spring.context;
    requires spring.core;
    requires spring.data.cassandra;
}
