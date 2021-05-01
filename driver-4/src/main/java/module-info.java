// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

module com.azure.cosmos.cassandra {

    exports com.azure.cosmos.cassandra;
    exports com.azure.cosmos.cassandra.implementation to com.azure.cosmos.cassandra.config;

    requires com.datastax.oss.driver.core;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
}
