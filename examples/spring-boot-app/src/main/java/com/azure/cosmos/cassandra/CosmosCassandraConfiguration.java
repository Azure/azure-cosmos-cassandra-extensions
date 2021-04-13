// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.SessionBuilderConfigurer;

import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.DNS_EXPIRY_TIME;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.GLOBAL_ENDPOINT;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.READ_DATACENTER;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.WRITE_DATACENTER;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.FIXED_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.GROWING_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.MAX_RETRIES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_USER_NAME;

/**
 * Configures a Cassandra client application's {@link com.datastax.oss.driver.api.core.CqlSession CqlSession} when it's
 * connected to a Cosmos DB Cassandra API instance.
 */
@Configuration
public abstract class CosmosCassandraConfiguration extends AbstractCassandraConfiguration {

    // region Fields

    private static final int PORT = 10350;

    // endregion

    // region Methods

    public int getLoadBalancingDnsExpiryTime() {
        return DNS_EXPIRY_TIME.getDefaultValue(Integer.class);
    }

    public String getLoadBalancingGlobalEndpoint() {
        return GLOBAL_ENDPOINT.getDefaultValue(String.class);
    }

    public String getLoadBalancingReadDatacenter() {
        return READ_DATACENTER.getDefaultValue(String.class);
    }

    public String getLoadBalancingWriteDatacenter() {
        return WRITE_DATACENTER.getDefaultValue(String.class);
    }

    @NonNull
    protected abstract String getAuthPassword();

    @NonNull
    protected abstract String getAuthUsername();

    @Override
    protected String getLocalDataCenter() {
        return null;
    }

    @Override
    protected int getPort() {
        return PORT;
    }

    protected int getRetryFixedBackoffTime() {
        return FIXED_BACKOFF_TIME.getDefaultValue(Integer.class);
    }

    protected int getRetryGrowingBackoffTime() {
        return GROWING_BACKOFF_TIME.getDefaultValue(Integer.class);
    }

    protected int getRetryMaxRetries() {
        return MAX_RETRIES.getDefaultValue(Integer.class);
    }

    @Override
    protected SessionBuilderConfigurer getSessionBuilderConfigurer() {
        return new CosmosCassandraSessionBuilderConfigurer(this);
    }

    // endregion

    // region Types

    private static class CosmosCassandraSessionBuilderConfigurer implements SessionBuilderConfigurer {

        private final CosmosCassandraConfiguration configuration;

        CosmosCassandraSessionBuilderConfigurer(final CosmosCassandraConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        @NonNull
        public CqlSessionBuilder configure(final CqlSessionBuilder builder) {

            return builder.withConfigLoader(DriverConfigLoader.programmaticBuilder()
                // Credentials
                .withString(AUTH_PROVIDER_USER_NAME, this.configuration.getAuthUsername())
                .withString(AUTH_PROVIDER_PASSWORD, this.configuration.getAuthPassword())
                // Load balancing policy options
                .withInt(DNS_EXPIRY_TIME, this.configuration.getLoadBalancingDnsExpiryTime())
                .withString(GLOBAL_ENDPOINT, this.configuration.getLoadBalancingGlobalEndpoint())
                .withString(READ_DATACENTER, this.configuration.getLoadBalancingReadDatacenter())
                .withString(WRITE_DATACENTER, this.configuration.getLoadBalancingWriteDatacenter())
                // Retry policy options
                .withInt(MAX_RETRIES, this.configuration.getRetryMaxRetries())
                .withInt(FIXED_BACKOFF_TIME, this.configuration.getRetryFixedBackoffTime())
                .withInt(GROWING_BACKOFF_TIME, this.configuration.getRetryGrowingBackoffTime())
                .build());
        }
    }

    // endregion
}
