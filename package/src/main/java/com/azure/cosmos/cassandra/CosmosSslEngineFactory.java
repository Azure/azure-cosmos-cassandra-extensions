// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;

public class CosmosSslEngineFactory extends ProgrammaticSslEngineFactory {

    public CosmosSslEngineFactory(final DriverContext driverContext) {
        super(createSslContext(driverContext));
    }

    protected static SSLContext createSslContext(final DriverContext driverContext) {

        final DriverExecutionProfile config = driverContext.getConfig().getDefaultProfile();

        try {
            if (config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PATH)
                || config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PATH)) {

                final SSLContext context = SSLContext.getInstance("SSL");

                // Initialize truststore, if it's configured

                final TrustManagerFactory tmf;

                if (config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PATH)) {

                    final Path truststorePath = Paths.get(config.getString(DefaultDriverOption.SSL_TRUSTSTORE_PATH));

                    try (final InputStream inputStream = Files.newInputStream(truststorePath)) {
                        final KeyStore keyStore = KeyStore.getInstance("JKS");
                        final char[] password = config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD)
                            ? config.getString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD).toCharArray()
                            : null;
                        keyStore.load(inputStream, password);
                        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        tmf.init(keyStore);
                    }
                } else {
                    tmf = null;
                }

                // Initialize keystore, if it's configured

                final KeyManagerFactory kmf;

                if (config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PATH)) {

                    final Path keyStorePath = Paths.get(config.getString(DefaultDriverOption.SSL_KEYSTORE_PATH));

                    try (final InputStream inputStream = Files.newInputStream(keyStorePath)) {
                        final KeyStore keyStore = KeyStore.getInstance("JKS");
                        final char[] password =
                            config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PASSWORD)
                                ? config.getString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD).toCharArray()
                                : null;
                        keyStore.load(inputStream, password);
                        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(keyStore, password);
                    }
                } else {
                    kmf = null;
                }

                context.init(
                    kmf != null ? kmf.getKeyManagers() : null,
                    tmf != null ? tmf.getTrustManagers() : null,
                    new SecureRandom());

                return context;
            } else {
                // if both keystore and truststore aren't configured, use default SSLContext.
                return SSLContext.getDefault();
            }
        } catch (final Exception error) {
            throw new IllegalStateException("Failed to initialize SSL context", error);
        }
    }
}

