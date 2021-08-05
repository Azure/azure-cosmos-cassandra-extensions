// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.azure.cosmos.cassandra.CosmosJson;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.annotation.JsonAppend.Prop;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.VirtualBeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.databind.util.Annotations;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Properties;

/**
 * A Json serialization module with serializers and mix-ins for logging DataStax Java Driver 4 diagnostic strings.
 * <p>
 * This module is registered for use by {@link CosmosJson}. The serialization and mix-in types that it contains augment,
 * filter, and format properties for various DataStax Java Driver 4 classes. They provide access to driver state that is
 * otherwise tedious to obtain. They are useful in log messages and while debugging when detailed diagnostic strings are
 * desirable.
 */
public final class CosmosJsonModule extends SimpleModule {

    // region Fields

    public static final Logger LOG = LoggerFactory.getLogger(CosmosJsonModule.class);
    private static final long serialVersionUID = 6126132601669760424L;

    // endregion

    // region Constructors

    /**
     * Initializes a newly created {@link CosmosJsonModule}.
     * <p>
     * A single instance of this class is constructed by {@link CosmosJson}. No other uses are supported.
     */
    public CosmosJsonModule() {

        super(CosmosJsonModule.class.getSimpleName(), getModuleVersion());

        this.addSerializer(Duration.class, ToStringSerializer.instance)
            .addSerializer(EndPoint.class, ToStringSerializer.instance)
            .addSerializer(Instant.class, ToStringSerializer.instance)
            .addSerializer(StackTraceElement.class, ToStringSerializer.instance)
            .addSerializer(Version.class, ToStringSerializer.instance);

        this.addMixInsAndSerializers(CosmosJsonModule.class.getClasses());
    }

    // endregion

    // region Privates

    @SuppressFBWarnings(value = {
        "NP_LOAD_OF_KNOWN_NULL_VALUE",
        "RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE"
    }, justification = "False alarm on Java 11")
    private static com.fasterxml.jackson.core.Version getModuleVersion() {

        final ClassLoader loader = CosmosJson.class.getClassLoader();

        try (InputStream stream = loader.getResourceAsStream("maven-coordinates.properties")) {

            if (stream == null) {
                return com.fasterxml.jackson.core.Version.unknownVersion();
            }

            final Properties mavenCoordinates = new Properties();
            mavenCoordinates.load(stream);

            final String artifactId = mavenCoordinates.getProperty("artifactId");
            final String groupId = mavenCoordinates.getProperty("groupId");
            final String version = mavenCoordinates.getProperty("version");

            final int major, minor, patch;
            final String snapshotInfo;

            if (version == null) {

                major = 0;
                minor = 0;
                patch = 0;
                snapshotInfo = null;

            } else {

                final String[] parts = version.split("\\.", 3);

                major = parts.length > 0 ? Integer.parseUnsignedInt(parts[0]) : 0;
                minor = parts.length > 1 ? Integer.parseUnsignedInt(parts[1]) : 0;

                if (parts.length < 3) {
                    patch = 0;
                    snapshotInfo = null;
                } else {
                    final int index = parts[2].indexOf('-');
                    if (index < 0) {
                        patch = Integer.parseUnsignedInt(parts[3]);
                        snapshotInfo = null;
                    } else {
                        patch = Integer.parseUnsignedInt(parts[2].substring(0, index));
                        snapshotInfo = parts[2].substring(index + 1);
                    }
                }
            }

            return new com.fasterxml.jackson.core.Version(major, minor, patch, snapshotInfo, groupId, artifactId);

        } catch (final IOException error) {
            LOG.warn("Failed to load maven-coordinates.properties due to ", error);
            return com.fasterxml.jackson.core.Version.unknownVersion();
        }
    }

    @SuppressWarnings("unchecked")
    private void addMixInsAndSerializers(@NonNull final Class<?>[] classes) {

        final ExceptionInInitializerError error = new ExceptionInInitializerError();

        for (final Class<?> cls : classes) {
            try {
                if (JsonSerializer.class.isAssignableFrom(cls)) {
                    final JsonSerializer<Object> serializer = (JsonSerializer<Object>) cls
                        .getDeclaredField("INSTANCE")
                        .get(null);
                    this.addSerializer(serializer.handledType(), serializer);
                } else if (cls.isAnnotationPresent(JsonAppend.class)) {
                    try {
                        final Object handledType = cls.getDeclaredField("HANDLED_TYPE").get(null);
                        if (handledType.getClass() == Class.class) {
                            this.setMixInAnnotation((Class<?>) handledType, cls);
                        }
                    } catch (final NoSuchFieldException ignored) {
                        // nothing to do
                    }
                }
            } catch (final IllegalAccessException | NoSuchFieldException exception) {
                error.addSuppressed(exception);
            }
        }

        if (error.getSuppressed().length > 0) {
            LOG.error("[{}] Class initialization failed due to: ", CosmosJson.class, error);
            throw error;
        }
    }

    // endregion

    // region Types

    /**
     * A mix-in for serializing {@link AuthProvider} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class AuthProviderMixIn {
        public static final Class<AuthProvider> HANDLED_TYPE = AuthProvider.class;
    }

    /**
     * A mix-in for serializing {@link BatchStatement} instances to JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(value = { "batchType", "statements" }, alphabetic = true)
    public abstract static class BatchStatementMixIn extends RequestMixIn {
        public static final Class<BatchStatement> HANDLED_TYPE = BatchStatement.class;

        /**
         * Specifies that {@link BatchStatement#iterator()} should be represented as a list-valued property named {@code
         * "statements"}.
         *
         * @return An iterator over the set of statements in this {@link BatchStatement}.
         */
        @JsonProperty("statements")
        public abstract Iterator<BatchableStatement<?>> iterator();
    }

    /**
     * A mix-in for serializing {@link DriverContext} instances to JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(value = { "sessionName", "startupOptions" }, alphabetic = true)
    @JsonIncludeProperties({
        "authProvider",
        "loadBalancingPolicies",
        "reconnectionPolicy",
        "requestThrottler",
        "requestTracker",
        "retryPolicies",
        "sessionName",
        "speculativeExecutionPolicies",
        "startupOptions" })
    public abstract static class DriverContextMixIn {
        public static final Class<DriverContext> HANDLED_TYPE = DriverContext.class;
    }

    /**
     * A mix-in for serializing {@link com.datastax.oss.driver.api.core.cql.ExecutionInfo} instances to JSON.
     */
    @JsonAppend()
    @JsonIgnoreProperties({ "incomingPayload", "pagingState", "queryTrace", "queryTraceAsync", "safePagingState" })
    public abstract static class ExecutionInfoMixIn {
        public static final Class<ExecutionInfo> HANDLED_TYPE = ExecutionInfo.class;
    }

    /**
     * A mix-in for serializing {@link LoadBalancingPolicy} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class LoadBalancingPolicyMixIn {
        public static final Class<LoadBalancingPolicy> HANDLED_TYPE = LoadBalancingPolicy.class;
    }

    /**
     * A mix-in for serializing {@link MetricRegistry} instances to JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(alphabetic = true)
    @JsonIgnoreProperties({ "metrics", "names" })
    public abstract static class MetricRegistryMixIn {
        public static final Class<MetricRegistry> HANDLED_TYPE = MetricRegistry.class;
    }

    /**
     * A mix-in for serializing {@link Node} instances to JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(value = { "endPoint", "datacenter", "distance", "hostId", "schemaVersion" }, alphabetic = true)
    @JsonIncludeProperties({
        "cassandraVersion",
        "datacenter",
        "distance",
        "endPoint",
        "hostId",
        "openConnections",
        "reconnecting",
        "schemaVersion",
        "state",
        "upSinceMillis" })
    public abstract static class NodeMixIn {
        public static final Class<Node> HANDLED_TYPE = Node.class;
    }

    /**
     * A mix-in for serializing {@link ReconnectionPolicy} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class ReconnectionPolicyMixIn {
        public static final Class<ReconnectionPolicy> HANDLED_TYPE = ReconnectionPolicy.class;
    }

    /**
     * A mix-in for serializing {@link Request} instances to JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(value = { "query", "namedValues", "positionalValues" }, alphabetic = true)
    @JsonIgnoreProperties({ "customPayload", "routingKey" })
    public abstract static class RequestMixIn {
        public static final Class<Request> HANDLED_TYPE = Request.class;
    }

    /**
     * A mix-in for serializing {@link RequestThrottler} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class RequestThrottlerMixIn {
        public static final Class<RequestThrottler> HANDLED_TYPE = RequestThrottler.class;
    }

    /**
     * A mix-in for serializing {@link RequestTracker} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class RequestTrackerMixIn {
        public static final Class<RequestTracker> HANDLED_TYPE = RequestTracker.class;
    }

    /**
     * A mix-in for serializing {@link RetryPolicy} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class RetryPolicyMixin {
        public static final Class<RetryPolicy> HANDLED_TYPE = RetryPolicy.class;
    }

    /**
     * A mix-in for serializing {@link Session} instances to JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(value = { "name" }, alphabetic = true)
    @JsonIncludeProperties({ "name", "context", "keyspace", "metadata", "metrics" })
    public abstract static class SessionMixIn {
        public static final Class<Session> HANDLED_TYPE = Session.class;
    }

    /**
     * A mix-in for serializing {@link Snapshot} instances to JSON.
     */
    @JsonAppend
    @JsonIgnoreProperties({ "values" })
    @JsonPropertyOrder(alphabetic = true)
    public abstract static class SnapshotMixIn {
        public static final Class<Snapshot> HANDLED_TYPE = Snapshot.class;
    }

    /**
     * A mix-in for serializing {@link SpeculativeExecutionPolicy} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
    public abstract static class SpeculativeExecutionPolicyMixIn {
        public static final Class<SpeculativeExecutionPolicy> HANDLED_TYPE = SpeculativeExecutionPolicy.class;
    }

    /**
     * A mix-in for serializing {@link Throwable} instances to JSON.
     */
    @JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "error", type = String.class), prepend = true)
    @JsonPropertyOrder(value = { "cause", "message", "stackTrace", "suppressed" }, alphabetic = true)
    @JsonIgnoreProperties({ "localizedMessage" })
    public abstract static class ThrowableMixIn {
        public static final Class<Throwable> HANDLED_TYPE = Throwable.class;
    }

    /**
     * A {@link VirtualBeanPropertyWriter property writer} for adding the {@link Class#getName class name} to the JSON
     * representation of an object.
     * <p>
     * Use it with {@link JsonAppend} to add virtual properties in addition to regular ones.
     *
     * @see ThrowableMixIn
     */
    public static final class TypePropertyWriter extends VirtualBeanPropertyWriter {

        private static final long serialVersionUID = -8203269538522149446L;

        /**
         * Initializes a newly created {@link TypePropertyWriter}.
         * <p>
         * Jackson requires a parameterless constructor for all {@link PropertyWriter} classes.
         */
        public TypePropertyWriter() {
            super();
        }

        /**
         * Initializes a newly created {@link TypePropertyWriter}.
         * <p>
         * Jackson requires a constructor with these parameters for all {@link PropertyWriter} classes.
         *
         * @param propertyDefinition A property definition.
         * @param annotations        The annotations present on the property.
         * @param javaType           The {@link JavaType} of the property value to be written.
         */
        public TypePropertyWriter(
            final BeanPropertyDefinition propertyDefinition,
            final Annotations annotations,
            final JavaType javaType) {

            super(propertyDefinition, annotations, javaType);
        }

        /**
         * A factory method for constructing a {@link TypePropertyWriter}.
         *
         * @param mapperConfig       Mapper configuration.
         * @param annotatedClass     An annotated class.
         * @param propertyDefinition A property definition.
         * @param javaType           The {@link JavaType} of the property value to be written.
         *
         * @return A newly created ClassNamePropertyWriter instance.
         */
        @Override
        public VirtualBeanPropertyWriter withConfig(
            final MapperConfig<?> mapperConfig,
            final AnnotatedClass annotatedClass,
            final BeanPropertyDefinition propertyDefinition,
            final JavaType javaType) {

            return new TypePropertyWriter(propertyDefinition, annotatedClass.getAnnotations(), javaType);
        }

        @Override
        protected Object value(
            final Object bean,
            final JsonGenerator generator,
            final SerializerProvider provider) {

            return bean.getClass().getName();
        }
    }

    // endregion
}
