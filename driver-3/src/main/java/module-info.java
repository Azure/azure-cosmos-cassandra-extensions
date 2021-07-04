// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

module com.azure.cosmos.cassandra {

    exports com.azure.cosmos.cassandra.implementation.serializer to com.fasterxml.jackson.databind;
    opens com.azure.cosmos.cassandra.implementation.serializer to com.fasterxml.jackson.databind;
    exports com.azure.cosmos.cassandra;

    // Named modules

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;

    // Automatic modules

    requires cassandra.driver.core;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
}
