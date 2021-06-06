// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.annotation.JsonAppend.Prop;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.VirtualBeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.databind.util.Annotations;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Utility class for serializing and deserializing JSON.
 * <p>
 * This class is used in {@linkplain Object#toString toString} implementations and test code.
 */
public final class Json {

    private Json() {
        throw new UnsupportedOperationException();
    }

    // region Fields

    private static final Logger LOG = LoggerFactory.getLogger(Json.class);

    private static final SimpleFilterProvider FILTER_PROVIDER = new SimpleFilterProvider();

    private static final SimpleModule MODULE = new SimpleModule()
        .addSerializer(Duration.class, ToStringSerializer.instance)
        .addSerializer(Instant.class, ToStringSerializer.instance);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(MODULE)
        .setFilterProvider(FILTER_PROVIDER)
        .addMixIn(Throwable.class, ThrowableMixIn.class);

    private static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writer();
    private static final ConcurrentHashMap<Class<?>, String> SIMPLE_CLASS_NAMES = new ConcurrentHashMap<>();

    /**
     * Returns the {@link SimpleModule module} used by this class.
     * <p>
     * This is useful for adding custom serializers using the {@link SimpleModule#addSerializer} method.
     *
     * @return The {@link SimpleModule module} used by this class.
     */
    public static SimpleModule module() {
        return MODULE;
    }

    // endregion

    // region

    /**
     * Returns the {@link ObjectMapper object mapper} used by this class.
     *
     * @return The {@link ObjectMapper object mapper} used by this class.
     */
    public static ObjectMapper objectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * Read a JSON value of the given type from a file.
     *
     * @param file The file from which to read the JSON value.
     * @param type A class modeling the value type to read.
     * @param <T>  The type of value to read.
     *
     * @return A reference to the value read.
     *
     * @throws IOException if a low-level I/O problem (unexpected end-of-input, network error) occurs.
     */
    public static <T> T readValue(@NonNull final File file, @NonNull final Class<T> type) throws IOException {
        requireNonNull(file, "expected non-null file");
        requireNonNull(type, "expected non-null type");
        return OBJECT_MAPPER.readValue(file, type);
    }

    /**
     * Read a JSON value of the given type from a stream.
     *
     * @param stream The stream from which to read the JSON value.
     * @param type   A class modeling the value type to read.
     * @param <T>    The type of value to read.
     *
     * @return A reference to the value read.
     *
     * @throws IOException if a low-level I/O problem (unexpected end-of-input, network error) occurs.
     */
    public static <T> T readValue(@NonNull final InputStream stream, @NonNull final Class<T> type) throws IOException {
        requireNonNull(stream, "expected non-null stream");
        requireNonNull(type, "expected non-null type");
        return OBJECT_MAPPER.readValue(stream, type);
    }

    /**
     * Read a JSON value of the given type from a string.
     *
     * @param string The string from which to read the JSON value.
     * @param type   A class modeling the value type to read.
     * @param <T>    The type of value to read.
     *
     * @return A reference to the value read.
     *
     * @throws IOException if a low-level I/O problem (unexpected end-of-input, network error) occurs.
     */
    public static <T> T readValue(@NonNull final String string, @NonNull final Class<T> type) throws IOException {
        requireNonNull(string, "expected non-null string");
        requireNonNull(type, "expected non-null type");
        return OBJECT_MAPPER.readValue(string, type);
    }

    /**
     * Converts the given value to JSON.
     *
     * @param value The value to convert.
     *
     * @return A JSON string representing the {@code value}.
     */
    @NonNull
    public static String toJson(@Nullable final Object value) {
        try {
            return OBJECT_WRITER.writeValueAsString(value);
        } catch (final JsonProcessingException error) {
            LOG.debug("could not convert {} value to JSON due to:", value != null ? value.getClass() : null, error);
            try {
                return "{\"error\":" + OBJECT_WRITER.writeValueAsString(error.toString()) + '}';
            } catch (final JsonProcessingException exception) {
                return "null";
            }
        }
    }

    /**
     * Converts the given value to a string of the form <i>&lt;simple-class-name&gt;</i><b>("</b><i>&lt;
     * json-string&gt;</i><b>)</b>").
     * <p>
     * This method is frequently used {@linkplain Object#toString toString} implementations.
     *
     * @param value The value to convert.
     *
     * @return A string of the form <i>&lt;simple-class-name&gt;</i><b>("</b><i>&lt;json-string&gt;</i><b>)</b>").
     */
    @NonNull
    public static String toString(@Nullable final Object value) {
        if (value != null) {
            final String name = SIMPLE_CLASS_NAMES.computeIfAbsent(value.getClass(), Class::getSimpleName);
            return name + '(' + toJson(value) + ')';
        }
        return "null";
    }

    /**
     * Returns The single object writer instantiated by this class.
     * <p>
     * This method may be used for specialized serialization tasks. It is less expensive than instantiating one of your
     * own and ensures that values are serialized consistently.
     *
     * @return The object writer instantiated by this class.
     */
    @NonNull
    public static ObjectWriter writer() {
        return OBJECT_WRITER;
    }

    @SuppressWarnings("SameParameterValue")
    static void registerPropertyFilter(final Class<?> type, final Class<? extends PropertyFilter> filter) {

        requireNonNull(type, "type");
        requireNonNull(filter, "filter");

        try {
            FILTER_PROVIDER.addFilter(type.getSimpleName(), filter.getDeclaredConstructor().newInstance());
        } catch (final ReflectiveOperationException error) {
            throw new IllegalStateException(error);
        }
    }

    // endregion

    // region Types

    private static class StackTraceSerializer extends StdSerializer<StackTraceElement[]> {

        private static final StackTraceSerializer INSTANCE = new StackTraceSerializer();
        private static final long serialVersionUID = 1526311794842313958L;

        private StackTraceSerializer() {
            super(StackTraceElement[].class);
        }

        @Override
        public void serialize(
            final StackTraceElement[] value,
            final JsonGenerator generator,
            final SerializerProvider provider) throws IOException {

            final StringBuilder builder = new StringBuilder();

            for (final StackTraceElement element : value) {
                builder.append("\tat ");
                builder.append(element);
                builder.append('\n');
            }

            generator.writeString(builder.toString());
        }
    }

    @JsonAppend(props = @Prop(value = ThrowableMixIn.Writer.class, name = "error", type = String.class), prepend = true)
    @JsonPropertyOrder({ "message", "cause", "stackTrace" })
    @JsonIgnoreProperties({ "localizedMessage" })
    private abstract static class ThrowableMixIn {

        @JsonSerialize(using = StackTraceSerializer.class)
        public abstract StackTraceElement[] getStackTrace();

        private static class Writer extends VirtualBeanPropertyWriter {

            private static final long serialVersionUID = -8203269538522149446L;

            public Writer() {
                super();
            }

            public Writer(
                final BeanPropertyDefinition propertyDefinition,
                final Annotations annotations,
                final JavaType javaType) {

                super(propertyDefinition, annotations, javaType);
            }

            @Override
            public VirtualBeanPropertyWriter withConfig(
                final MapperConfig<?> mapperConfig,
                final AnnotatedClass annotatedClass,
                final BeanPropertyDefinition propertyDefinition,
                final JavaType javaType) {

                return new Writer(propertyDefinition, annotatedClass.getAnnotations(), javaType);
            }

            @Override
            protected Object value(
                final Object bean,
                final JsonGenerator generator,
                final SerializerProvider provider) {

                return bean.getClass().getName();
            }
        }
    }

    // endregion
}
