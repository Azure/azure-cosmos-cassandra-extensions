// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

module com.azure.cosmos.cassandra {

    exports com.azure.cosmos.cassandra;

    requires com.datastax.oss.driver.core;
    requires com.github.spotbugs.annotations;
    requires org.slf4j;
}
