// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.module.SimpleModule;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SimpleModule MODULE = new SimpleModule();
    private static final ConcurrentHashMap<Class<?>, String> SIMPLE_CLASS_NAMES = new ConcurrentHashMap<>();

    private static final JsonRegistrar REGISTRAR = new JsonRegistrar();
    private static final ObjectWriter WRITER;

    static {
        REGISTRAR.registerSerializers();
        MAPPER.registerModule(MODULE);
        WRITER = MAPPER.writer();
    }

    // endregion

    // region Methods

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

    /**
     * Returns the {@link ObjectMapper object mapper} used by this class.
     *
     * @return The {@link ObjectMapper object mapper} used by this class.
     */
    public static ObjectMapper objectMapper() {
        return MAPPER;
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
        return MAPPER.readValue(file, type);
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
        return MAPPER.readValue(stream, type);
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
        return MAPPER.readValue(string, type);
    }

    /**
     * Registers a serializer for a type.
     *
     * @param type          A {@link Class class}.
     * @param serializer    A {@link JsonSerializer serializer}.
     * @param <T>           The type of the class to be serialized.
     *
     * @return              A reference to the module to which {@code serializer} was added. This is the value of
     *                      {@link #module}.
     */
    public static <T> SimpleModule addSerializer(
        @NonNull final Class<? extends T> type,
        @NonNull final JsonSerializer<T> serializer) {

        return MODULE.addSerializer(type, serializer);
    }

    /**
     * Registers all the serializers in the named package, including mix-in classes.
     * <p>
     * Instances of all {@link JsonSerializer} types are added to {@link #module}. All mix-in classes (i.e., classes
     * annotated with {@link JsonAppend}) are added to {@link #objectMapper}. Nested classes are not considered.
     * Mix-in classes must contain this static member or must be added to {@link #objectMapper} manually:
     * <pre>{@code public static final Class<?> HANDLED_TYPE = handled_type;}</pre>
     *
     * @param packageName the name of the package containing serializers and mix-in classes to be added.
     */
    @SuppressWarnings("unchecked")
    public static void addSerializersAndMixIns(@NonNull final String packageName) {

        final String packagePath = packageName.replaceAll("[.]", "/");

        try (InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream(packagePath)) {

            if (stream == null) {
                final ExceptionInInitializerError error = new ExceptionInInitializerError(
                    "expected resource stream for package"
                        + packagePath);
                LOG.error("[{}] Class initialization failed due to: ", Json.class, error);
                throw error;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {

                final ExceptionInInitializerError error = new ExceptionInInitializerError();

                reader.lines()
                    .filter(line -> line.endsWith(".class")).map(line -> {

                        final String simpleClassName = line.substring(0, line.lastIndexOf('.'));

                        try {
                            return Class.forName(packageName + "." + simpleClassName);
                        } catch (final ClassNotFoundException exception) {
                            error.addSuppressed(exception);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull).forEachOrdered(cls -> {

                        final String className = cls.getName();

                        try {

                            if (JsonSerializer.class.isAssignableFrom(cls)) {

                                final JsonSerializer<Object> serializer = (JsonSerializer<Object>) cls
                                    .getDeclaredField("INSTANCE")
                                    .get(null);

                                MODULE.addSerializer(serializer.handledType(), serializer);

                            } else if (cls.isAnnotationPresent(JsonAppend.class)) {

                                try {
                                    final Object value = cls.getDeclaredField("HANDLED_TYPE").get(null);
                                    if (value.getClass() == Class.class) {
                                        MAPPER.addMixIn((Class<?>) value, cls);
                                    }
                                } catch (final NoSuchFieldException ignored) {
                                    // nothing to do
                                }
                            }
                        } catch (IllegalAccessException | NoSuchFieldException exception) {
                            error.addSuppressed(exception);
                        }
                    });

                if (error.getSuppressed().length > 0) {
                    LOG.error("[{}] Class initialization failed due to: ", Json.class, error);
                    throw error;
                }
            }

        } catch (final IOException error) {
            LOG.error("[{}] Class initialization failed due to: ", Json.class, error);
            throw new ExceptionInInitializerError(error);
        }
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
            return WRITER.writeValueAsString(value);
        } catch (final JsonProcessingException error) {
            LOG.debug("could not convert {} value to JSON due to:", value != null ? value.getClass() : null, error);
            try {
                return "{\"error\":" + WRITER.writeValueAsString(error.toString()) + '}';
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
        return WRITER;
    }

    // endregion
}
