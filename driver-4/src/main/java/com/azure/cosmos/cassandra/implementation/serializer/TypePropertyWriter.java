// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.VirtualBeanPropertyWriter;
import com.fasterxml.jackson.databind.util.Annotations;

/**
 * A {@link VirtualBeanPropertyWriter} for adding the {@link Class#getName class name} to the JSON representation of an
 * object.
 * <p>
 * Use it with {@link JsonAppend} to add virtual properties in addition to regular ones.
 *
 * @see ThrowableMixIn
 */
public final class TypePropertyWriter extends VirtualBeanPropertyWriter {

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
     * Jackson requires a constructor with these parameters for all {@link PropertyWriter} classes. This method
     *
     * @param propertyDefinition    A property definition.
     * @param annotations           The annotations present on the property.
     * @param javaType              The {@link JavaType} of the property value to be written.
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
     * @param mapperConfig          Mapper configuration.
     * @param annotatedClass        An annotated class.
     * @param propertyDefinition    A property definition.
     * @param javaType              The {@link JavaType} of the property value to be written.
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
