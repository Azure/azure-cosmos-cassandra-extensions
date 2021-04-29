// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.config;

import com.azure.cosmos.cassandra.CosmosLoadBalancingPolicy;
import com.azure.cosmos.cassandra.CosmosLoadBalancingPolicyOption;
import com.azure.cosmos.cassandra.CosmosRetryPolicy;
import com.azure.cosmos.cassandra.CosmosRetryPolicyOption;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.SessionBuilderConfigurer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static final Logger LOG = LoggerFactory.getLogger(CosmosCassandraConfiguration.class);
    private static final int PORT = 10350;

    // endregion

    // region Methods

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#MULTI_REGION_WRITES}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#MULTI_REGION_WRITES}.
     */
    public boolean getLoadBalancingPolicyMultiRegionWrites() {
        return MULTI_REGION_WRITES.getDefaultValue(Boolean.class);
    }

    /**
     * Returns the value to set for {@link CosmosLoadBalancingPolicyOption#PREFERRED_REGIONS}.
     *
     * @return The value to set for {@link CosmosLoadBalancingPolicyOption#PREFERRED_REGIONS}.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public List<String> getLoadBalancingPolicyPreferredRegions() {
        return PREFERRED_REGIONS.getDefaultValue(List.class);
    }

    @Override
    public String toString() {

        final char[] password = new char[this.getAuthPassword().length()];
        Arrays.fill(password, '*');

        return "azure.cosmos.cassandra:\n"
            + "  base-packages: " + Arrays.toString(this.getEntityBasePackages()) + '\n'
            + "  keyspace: " + this.getKeyspaceName() + '\n'
            + "  contact-point: " + this.getContactPoints() + '\n'
            + "  auth-provider: \n"
            + "    username: " + this.getAuthUsername() + '\n'
            + "    password: " + new String(password) + '\n'
            + "  load-balancing-policy:\n"
            + "    multi-region-writes: " + this.getLoadBalancingPolicyMultiRegionWrites() + '\n'
            + "    preferred-regions: " + this.getLoadBalancingPolicyPreferredRegions() + '\n'
            + "  retry-policy:\n"
            + "     max-retries: " + this.getRetryMaxRetries() + '\n'
            + "     fixed-backoff-time: " + this.getRetryFixedBackoffTime() + '\n'
            + "     growing-backoff-time: " + this.getRetryGrowingBackoffTime();
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
    @Nullable
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

            LOG.debug("{}({})", this.configuration.getClass().getName(), this.configuration);

            return builder.withConfigLoader(DriverConfigLoader.programmaticBuilder()

                // Credentials

                .withString(AUTH_PROVIDER_USER_NAME, this.configuration.getAuthUsername())
                .withString(AUTH_PROVIDER_PASSWORD, this.configuration.getAuthPassword())

                // Load balancing policy options

                .withBoolean(MULTI_REGION_WRITES, this.configuration.getLoadBalancingPolicyMultiRegionWrites())

                .withStringList(PREFERRED_REGIONS, nonNullOrElse(
                    this.configuration.getLoadBalancingPolicyPreferredRegions(),
                    Collections.emptyList()))

                // Retry policy options

                .withInt(MAX_RETRIES, this.configuration.getRetryMaxRetries())
                .withInt(FIXED_BACKOFF_TIME, this.configuration.getRetryFixedBackoffTime())
                .withInt(GROWING_BACKOFF_TIME, this.configuration.getRetryGrowingBackoffTime())
                .build());
        }

        private static <T> T nonNullOrElse(final T value, final T defaultValue) {
            return value != null ? value : defaultValue;
        }
    }

    // endregion
}
