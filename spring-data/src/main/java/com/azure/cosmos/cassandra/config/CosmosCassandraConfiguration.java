// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.config;

import com.azure.cosmos.cassandra.CosmosLoadBalancingPolicy;
import com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption;
import com.azure.cosmos.cassandra.CosmosRetryPolicy;
import com.azure.cosmos.cassandra.CosmosRetryPolicyOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.DriverConfigLoaderBuilderConfigurer;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.MULTI_REGION_WRITES;
import static com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption.PREFERRED_REGIONS;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.FIXED_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.GROWING_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.MAX_RETRIES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_USER_NAME;

/**
 * Spring Configuration class used to configure a Cassandra client application {@link
 * com.datastax.oss.driver.api.core.CqlSession CqlSession} connected to an Azure Cosmos DB Cassandra API instance. In
 * addition to the capabilities offered by
 * {@link org.springframework.data.cassandra.config.AbstractCassandraConfiguration
 * AbstractCassandraConfiguration} it enables you to configure credentials and Cosmos DB aware load balancing and retry
 * policy. Through its dependency on the {@code azure-cosmos-cassandra-driver-4-extensions} package, it also offers a
 * sensible set of default {@code datastax-java-driver} options for efficiently accessing a Cosmos DB Cassandra API
 * instance.
 *
 * @see CosmosLoadBalancingPolicy
 * @see CosmosRetryPolicy
 * @see
 * <a href="https://github.com/Azure/azure-cosmos-cassandra-extensions/blob/develop/java-driver-4/driver-4/">reference.conf</a>
 */
@Configuration
public abstract class CosmosCassandraConfiguration extends AbstractCassandraConfiguration {

    // region Fields

    private static final int PORT = 10350;

    // endregion

    // region Methods

    /**
     * Returns the value to set for {@link DefaultDriverOption#AUTH_PROVIDER_PASSWORD}.
     *
     * @return The value to set for {@link DefaultDriverOption#AUTH_PROVIDER_PASSWORD}.
     */
    @Nullable
    protected abstract String getAuthPassword();

    /**
     * Returns the value to set for {@link DefaultDriverOption#AUTH_PROVIDER_USER_NAME}.
     *
     * @return The value to set for {@link DefaultDriverOption#AUTH_PROVIDER_USER_NAME}.
     */
    @Nullable
    protected abstract String getAuthUsername();

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#MULTI_REGION_WRITES}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#MULTI_REGION_WRITES}.
     */
    @Nullable
    protected Boolean getLoadBalancingPolicyMultiRegionWrites() {
        return null;
    }

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#PREFERRED_REGIONS}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#PREFERRED_REGIONS}.
     */
    @SuppressFBWarnings("PZLA_PREFER_ZERO_LENGTH_ARRAYS")
    @Nullable
    protected String[] getLoadBalancingPolicyPreferredRegions() {
        return null;
    }

    @Override
    @Nullable
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
    @Nullable
    protected Integer getRetryFixedBackoffTime() {
        return null;
    }

    /**
     * Returns the value to set for {@link CosmosRetryPolicyOption#GROWING_BACKOFF_TIME}.
     *
     * @return The value to set for {@link CosmosRetryPolicyOption#GROWING_BACKOFF_TIME}.
     */
    @Nullable
    protected Integer getRetryGrowingBackoffTime() {
        return null;
    }

    /**
     * Returns the value to set for {@link CosmosRetryPolicyOption#MAX_RETRIES}.
     *
     * @return The value to set for {@link CosmosRetryPolicyOption#MAX_RETRIES}.
     */
    @Nullable
    protected Integer getRetryMaxRetries() {
        return null;
    }

    @Nullable
    protected DriverConfigLoaderBuilderConfigurer getDriverConfigLoaderBuilderConfigurer() {
        return new CosmosDriverConfigLoaderBuilderConfigurer(this);
    }

    // endregion

    // region Types

    private static class CosmosDriverConfigLoaderBuilderConfigurer implements DriverConfigLoaderBuilderConfigurer {

        private final CosmosCassandraConfiguration configuration;

        CosmosDriverConfigLoaderBuilderConfigurer(final CosmosCassandraConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public void configure(@NonNull final ProgrammaticDriverConfigLoaderBuilder builder) {

            // Credentials

            final String username = this.configuration.getAuthUsername();

            if (!(username == null || username.isEmpty())) {
                builder.withString(AUTH_PROVIDER_USER_NAME, username);
            }

            final String password = this.configuration.getAuthPassword();

            if (!(password == null || password.isEmpty())) {
                builder.withString(AUTH_PROVIDER_PASSWORD, password);
            }

            // Load balancing policy options

            final Boolean multiRegionWrites = this.configuration.getLoadBalancingPolicyMultiRegionWrites();

            if (multiRegionWrites != null) {
                builder.withBoolean(MULTI_REGION_WRITES, multiRegionWrites);
            }

            final String[] preferredRegions = this.configuration.getLoadBalancingPolicyPreferredRegions();

            if (preferredRegions != null) {
                builder.withStringList(PREFERRED_REGIONS, Arrays.stream(preferredRegions)
                    .filter(region -> !(region == null || region.isEmpty()))
                    .collect(Collectors.toList()));
            }

            // Retry policy options

            final Integer maxRetries = this.configuration.getRetryMaxRetries();

            if (maxRetries != null) {
                builder.withInt(MAX_RETRIES, maxRetries);
            }

            final Integer fixedBackoffTime = this.configuration.getRetryFixedBackoffTime();

            if (fixedBackoffTime != null) {
                builder.withInt(FIXED_BACKOFF_TIME, fixedBackoffTime);
            }

            final Integer growingBackoffTime = this.configuration.getRetryGrowingBackoffTime();

            if (growingBackoffTime != null) {
                builder.withInt(GROWING_BACKOFF_TIME, this.configuration.getRetryGrowingBackoffTime());
            }
        }
    }

    // endregion
}
