// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.azure.cosmos.cassandra.CosmosJson;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.VersionNumber;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
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
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
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
import java.util.Properties;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

/**
 * A Json serialization module with serializers and mix-ins for logging DataStax Java Driver 3 diagnostic strings.
 * <p>
 * This module is registered for use by {@link CosmosJson}. The serialization and mix-in types that it contains augment,
 * filter, and format properties for various DataStax Java Driver 3 classes. They provide access to driver state that is
 * otherwise tedious to obtain. They are useful in log messages and while debugging when detailed diagnostic strings are
 * desirable.
 */
public final class CosmosJsonModule extends SimpleModule {

    // region Fields

    public static final Logger LOG = LoggerFactory.getLogger(CosmosJsonModule.class);
    private static final long serialVersionUID = 1264833301420874793L;

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
            .addSerializer(VersionNumber.class, ToStringSerializer.instance);

        this.addMixInsAndSerializers(CosmosJsonModule.class.getClasses());
    }

    // endregion

    // region Privates

    @SuppressFBWarnings(value = {
        "NP_LOAD_OF_KNOWN_NULL_VALUE",
        "RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE"
    }, justification = "False alarm on Java 11")
    private static Version getModuleVersion() {

        final ClassLoader loader = CosmosJson.class.getClassLoader();

        try (InputStream stream = loader.getResourceAsStream("maven-coordinates.properties")) {

            if (stream == null) {
                return Version.unknownVersion();
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
            return Version.unknownVersion();
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
     * A {@link JsonSerializer} for serializing a {@link Cluster} object into JSON.
     */
    public static final class ClusterSerializer extends StdSerializer<Cluster> {

        public static final ClusterSerializer INSTANCE = new ClusterSerializer();
        private static final long serialVersionUID = -2084259636087581282L;

        private ClusterSerializer() {
            super(Cluster.class);
        }

        @Override
        public void serialize(
            @NonNull final Cluster value,
            @NonNull final JsonGenerator generator,
            @NonNull final SerializerProvider provider) throws IOException {

            requireNonNull(value, "expected non-null value");
            requireNonNull(value, "expected non-null generator");
            requireNonNull(value, "expected non-null provider");

            generator.writeStartObject();
            provider.defaultSerializeField("clusterName", value.getClusterName(), generator);
            provider.defaultSerializeField("closed", value.isClosed(), generator);

            Metrics metrics = null;

            if (value.isClosed()) {
                generator.writeNullField("metadata");
            } else {
                try {
                    // Side-effect: Cluster::getMetadata calls Metadata::init which throws, if the call fails
                    provider.defaultSerializeField("metadata", value.getMetadata(), generator);
                    metrics = value.getMetrics();
                } catch (final Throwable error) {
                    provider.defaultSerializeField("metadata", error, generator);
                }
            }

            provider.defaultSerializeField("metrics", metrics, generator);
            generator.writeEndObject();
        }
    }

    /**
     * A {@link JsonSerializer} for serializing a {@link Host} object into JSON.
     */
    public static final class HostSerializer extends StdSerializer<Host> {

        public static final HostSerializer INSTANCE = new HostSerializer();
        private static final long serialVersionUID = -7559845199616549188L;

        private HostSerializer() {
            super(Host.class);
        }

        @Override
        public void serialize(
            @NonNull final Host value,
            @NonNull final JsonGenerator generator,
            @NonNull final SerializerProvider provider) throws IOException {

            requireNonNull(value, "expected non-null value");
            requireNonNull(value, "expected non-null generator");
            requireNonNull(value, "expected non-null provider");

            final VersionNumber cassandraVersion = value.getCassandraVersion();

            generator.writeStartObject();

            provider.defaultSerializeField("endPoint", value.getEndPoint().toString(), generator);
            provider.defaultSerializeField("datacenter", value.getDatacenter(), generator);
            provider.defaultSerializeField("hostId", value.getHostId(), generator);
            provider.defaultSerializeField("state", value.getState(), generator);
            provider.defaultSerializeField("schemaVersion", value.getSchemaVersion(), generator);

            provider.defaultSerializeField("cassandraVersion", cassandraVersion == null
                    ? null
                    : cassandraVersion.toString(),
                generator);

            generator.writeEndObject();
        }
    }

    /**
     * A {@link JsonSerializer} for serializing a {@link Metadata} object into JSON.
     */
    public static final class MetadataSerializer extends StdSerializer<Metadata> {

        public static final MetadataSerializer INSTANCE = new MetadataSerializer();
        private static final long serialVersionUID = 2234571397397316058L;

        private MetadataSerializer() {
            super(Metadata.class);
        }

        @Override
        public void serialize(
            @NonNull final Metadata value,
            @NonNull final JsonGenerator generator,
            @NonNull final SerializerProvider provider) throws IOException {

            requireNonNull(value, "expected non-null value");
            requireNonNull(value, "expected non-null generator");
            requireNonNull(value, "expected non-null provider");

            generator.writeStartObject();
            provider.defaultSerializeField("clusterName", value.getClusterName(), generator);
            provider.defaultSerializeField("hosts", value.getAllHosts(), generator);
            generator.writeEndObject();
        }
    }

    /**
     * A mix-in for serializing a {@link Session} object into JSON.
     */
    @JsonAppend()
    @JsonIgnoreProperties("state")
    @JsonPropertyOrder({ "cluster", "closed", "loggedKeyspace" })
    public abstract static class SessionMixIn {
        public static final Class<Session> HANDLED_TYPE = Session.class;
    }

    /**
     * A mix-in for serializing a {@link Statement} object into JSON.
     */
    @JsonAppend()
    @JsonPropertyOrder(value = { "queryString", "tracing", "fetchSize", "readTimeoutMillis" }, alphabetic = true)
    @JsonIgnoreProperties({ "nowInSeconds", "outgoingPayload", "routingKey", "valueNames" })
    @JsonInclude(NON_NULL)
    public abstract static class StatementMixIn {
        public static final Class<Statement> HANDLED_TYPE = Statement.class;
    }

    /**
     * A mix-in for serializing a {@link Throwable} object into JSON.
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
