// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

/**
 * Configures a Cassandra client application's {@link com.datastax.oss.driver.api.core.CqlSession CqlSession} when it's
 * connected to a Cosmos DB Cassandra API instance.
 */
@Configuration
@EnableCassandraRepositories
public class CosmosCassandraConfiguration extends AbstractCassandraConfiguration {

    // region Fields

    private static final int PORT = 10350;

    @Value("${cassandra.base-packages}")
    private String basePackages;

    @Value("${cassandra.contact-point}")
    private String contactPoint;

    @Value("${cassandra.keyspace}")
    private String keySpace;

    // endregion

    // region Methods

    @Override
    @NonNull
    public String[] getEntityBasePackages() {
        return new String[] { this.basePackages };
    }

    @Override
    @NonNull
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

    @Override
    @NonNull
    protected String getContactPoints() {
        return this.contactPoint;
    }

    @Override
    @NonNull
    protected String getKeyspaceName() {
        return this.keySpace;
    }

    @Override
    protected int getPort() {
        return PORT;
    }

    // endregion
}
