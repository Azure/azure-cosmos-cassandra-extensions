// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/***
 * Module descriptor
 */
module com.azure.cosmos.cassandra {

    exports com.azure.cosmos.cassandra;
    opens com.azure.cosmos.cassandra;
    exports com.azure.cosmos.cassandra.implementation;
    opens com.azure.cosmos.cassandra.implementation;

    // Named modules

    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jdk8;

    // Automatic modules

    requires com.codahale.metrics;
    requires com.datastax.oss.driver.core;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
}
