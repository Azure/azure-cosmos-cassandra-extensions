// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.CosmosCassandraConfiguration;
import com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption;
import com.azure.cosmos.cassandra.CosmosRetryPolicyOption;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import java.util.Collections;
import java.util.List;

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

    @Value("${cosmos.cassandra.load-balancing-policy.dns-expiry-time:#{null}}")
    private Integer dnsExpiryTime;

    @Value("${cosmos.cassandra.load-balancing-policy.global-endpoint:}")
    private String globalEndpoint;

    @Value("${cosmos.cassandra.load-balancing-policy.read-datacenter:}")
    private String readDatacenter;

    @Value("${cosmos.cassandra.load-balancing-policy.write-datacenter:}")
    private String writeDatacenter;

    // endregion

    // region Retry policy options

    @Value("${cosmos.cassandra.retry-policy.fixed-backoff-time:#{null}}")
    private Integer fixedBackoffTime;

    @Value("${cosmos.cassandra.retry-policy.growing-backoff-time:#{null}}")
    private Integer growingBackoffTime;

    @Value("${cosmos.cassandra.retry-policy.max-retries:#{null}}")
    private Integer maxRetries;

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
    public int getLoadBalancingDnsExpiryTime() {
        return this.dnsExpiryTime == null
            ? CosmosLoadBalancingPolicyOption.DNS_EXPIRY_TIME.getDefaultValue(Integer.class)
            : this.dnsExpiryTime;
    }

    @Override
    @Nullable
    public String getLoadBalancingGlobalEndpoint() {
        return this.globalEndpoint;
    }

    @Override
    @Nullable
    public String getLoadBalancingReadDatacenter() {
        return this.readDatacenter;
    }

    @Override
    @Nullable
    public String getLoadBalancingWriteDatacenter() {
        return this.writeDatacenter;
    }

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

    protected int getRetryFixedBackoffTime() {
        return this.fixedBackoffTime == null
            ? CosmosRetryPolicyOption.FIXED_BACKOFF_TIME.getDefaultValue(Integer.class)
            : this.fixedBackoffTime;
    }

    protected int getRetryGrowingBackoffTime() {
        return this.growingBackoffTime == null
            ? CosmosRetryPolicyOption.GROWING_BACKOFF_TIME.getDefaultValue(Integer.class)
            : this.growingBackoffTime;
    }

    protected int getRetryMaxRetries() {
        return this.maxRetries == null
            ? CosmosRetryPolicyOption.MAX_RETRIES.getDefaultValue(Integer.class)
            : this.maxRetries;
    }

    // endregion

    // region Methods to ensure the application runs with a low profile and with minimum fuss

    @Override
    @NonNull
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

    @Override
    @NonNull
    protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
        return
            Collections.singletonList(CreateKeyspaceSpecification.createKeyspace(this.getKeyspaceName()).ifNotExists());
    }

    @Override
    @NonNull
    protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
        return Collections.emptyList();
    }

    // endregion
}
