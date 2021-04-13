// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
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
 * Base class for configuring a {@link com.datastax.oss.driver.api.core.CqlSession CqlSession} connected to a Cosmos DB
 * Cassandra API instance.
 */
@Configuration
public abstract class CosmosCassandraConfiguration extends AbstractCassandraConfiguration {

    // region Fields

    private static final int PORT = 10350;

    // endregion

    // region Methods

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#DNS_EXPIRY_TIME}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#DNS_EXPIRY_TIME}.
     */
    public int getLoadBalancingDnsExpiryTime() {
        return DNS_EXPIRY_TIME.getDefaultValue(Integer.class);
    }

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#GLOBAL_ENDPOINT}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#GLOBAL_ENDPOINT}.
     */
    public String getLoadBalancingGlobalEndpoint() {
        return GLOBAL_ENDPOINT.getDefaultValue(String.class);
    }

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#READ_DATACENTER}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#READ_DATACENTER}.
     */
    public String getLoadBalancingReadDatacenter() {
        return READ_DATACENTER.getDefaultValue(String.class);
    }

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#WRITE_DATACENTER}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#WRITE_DATACENTER}.
     */
    public String getLoadBalancingWriteDatacenter() {
        return WRITE_DATACENTER.getDefaultValue(String.class);
    }

    /**
     * Returns the value to set for {@link DefaultDriverOption#AUTH_PROVIDER_PASSWORD}.
     *
     * @return The value to set for {@link DefaultDriverOption#AUTH_PROVIDER_PASSWORD}.
     */
    @NonNull
    protected abstract String getAuthPassword();

    /**
     * Returns the value to set for {@link DefaultDriverOption#AUTH_PROVIDER_USER_NAME}.
     *
     * @return The value to set for {@link DefaultDriverOption#AUTH_PROVIDER_USER_NAME}.
     */
    @NonNull
    protected abstract String getAuthUsername();

    @Override
    protected String getLocalDataCenter() {
        return null;
    }

    /**
     * Returns the Cosmos DB Cassandra API port number.
     * <p>
     * The default is 10350.
     *
     * @return The Cosmos DB Cassandra API port number.
     */
    @Override
    protected int getPort() {
        return PORT;
    }

    /**
     * Returns the value to set for {@link CosmosRetryPolicyOption#FIXED_BACKOFF_TIME}.
     *
     * @return The value to set for {@link CosmosRetryPolicyOption#FIXED_BACKOFF_TIME}.
     */
    protected int getRetryFixedBackoffTime() {
        return FIXED_BACKOFF_TIME.getDefaultValue(Integer.class);
    }

    /**
     * Returns the value to set for {@link CosmosRetryPolicyOption#GROWING_BACKOFF_TIME}.
     *
     * @return The value to set for {@link CosmosRetryPolicyOption#GROWING_BACKOFF_TIME}.
     */
    protected int getRetryGrowingBackoffTime() {
        return GROWING_BACKOFF_TIME.getDefaultValue(Integer.class);
    }

    /**
     * Returns the value to set for {@link CosmosRetryPolicyOption#MAX_RETRIES}.
     *
     * @return The value to set for {@link CosmosRetryPolicyOption#MAX_RETRIES}.
     */
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
