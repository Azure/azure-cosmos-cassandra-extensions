// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.config.CosmosCassandraConfiguration;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import java.util.Collections;
import java.util.List;

import static org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification.createKeyspace;

/**
 * Spring Data Cassandra configuration that reads settings values from application.yaml.
 */
@Configuration
@EnableCassandraRepositories
public class ApplicationConfiguration extends CosmosCassandraConfiguration {

    // region Connection options

    @Value("${cosmos.cassandra.base-packages}")
    private String basePackages;

    @Value("${cosmos.cassandra.contact-point}")
    private String contactPoint;

    @Value("${cosmos.cassandra.keyspace}")
    private String keySpaceName;

    // endregion

    // region Authentication options

    @Value("${cosmos.cassandra.auth-provider.username}")
    private String username;

    @Value("${cosmos.cassandra.auth-provider.password}")
    private String password;

    // endregion

    // region Load balancing policy options

    @Value("${cosmos.cassandra.load-balancing-policy.multi-region-writes:#{null}}")
    private Boolean multiRegionWrites;

    @Value("${cosmos.cassandra.load-balancing-policy.preferred-regions:#{null}}")
    private String[] preferredRegions;

    // endregion

    // region Retry policy options

    @Value("${cosmos.cassandra.retry-policy.fixed-backoff-time:#{null}}")
    private Integer fixedBackoffTime;

    @Value("${cosmos.cassandra.retry-policy.growing-backoff-time:#{null}}")
    private Integer growingBackoffTime;

    @Value("${cosmos.cassandra.retry-policy.max-retries:#{null}}")
    private Integer maxRetries;

    // endregion

    // region Schema creation options

    @Value("${cosmos.cassandra.schema-action}")
    private SchemaAction schemaAction;

    // endregion

    // region Methods

    @Override
    @NonNull
    protected String getAuthPassword() {
        return this.password;
    }

    @Override
    @NonNull
    protected String getAuthUsername() {
        return this.username;
    }

    @Override
    @NonNull
    public String[] getEntityBasePackages() {
        return new String[] { this.basePackages };
    }


    @Override
    @Nullable
    public Boolean getLoadBalancingPolicyMultiRegionWrites() {
        return this.multiRegionWrites;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    @NonNull
    public String[] getLoadBalancingPolicyPreferredRegions() {
        return this.preferredRegions;
    }

    /**
     * Gets the contact points for the current session.
     * <p>
     * You should specify a single contact-point when connecting to a Cosmos DB Cassandra API instance: The global
     * endpoint address.
     *
     * @return The contact points for the current session.
     */
    @Override
    @NonNull
    protected String getContactPoints() {
        return this.contactPoint;
    }

    @Override
    @NonNull
    protected String getKeyspaceName() {
        return this.keySpaceName;
    }

    @Override
    @Nullable
    protected Integer getRetryFixedBackoffTime() {
        return this.fixedBackoffTime;
    }

    @Override
    @Nullable
    protected Integer getRetryGrowingBackoffTime() {
        return this.growingBackoffTime;
    }

    @Override
    @Nullable
    protected Integer getRetryMaxRetries() {
        return this.maxRetries;
    }

    // endregion

    /**
     * Gets a {@link ClassPathResource} for loading {@code application.conf} used to configure the DataStax Java Driver.
     * <p>
     * You will find this file in the source at {@code src/main/resources/application.conf}. This file is packaged with
     * the app at the root of its class path.
     *
     * @return A {@link ClassPathResource ClassPathResource} for loading {@code application.conf}.
     */
    @Override
    protected Resource getDriverConfigurationResource() {
        return new ClassPathResource("application.conf");
    }

    // region Methods to ensure the application runs with a low profile and with minimum fuss

    @Override
    @NonNull
    public SchemaAction getSchemaAction() {
        return this.schemaAction == null ? SchemaAction.NONE : this.schemaAction;
    }

    @Override
    @NonNull
    protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
        final CreateKeyspaceSpecification specification = createKeyspace(this.getKeyspaceName()).ifNotExists();
        return Collections.singletonList(specification);
    }

    @Override
    @NonNull
    protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
        return Collections.emptyList();
    }

    // endregion
}
