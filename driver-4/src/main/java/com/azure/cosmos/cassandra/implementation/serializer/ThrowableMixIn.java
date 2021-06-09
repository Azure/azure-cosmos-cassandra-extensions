// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing a {@link Throwable} object to JSON for use in log messages.
 */
@JsonAppend(
    props = @JsonAppend.Prop(value = ClassNamePropertyWriter.class, name = "error", type = String.class),
    prepend = true)
@JsonPropertyOrder(value = { "cause", "message", "stackTrace", "suppressed" }, alphabetic = true)
@JsonIgnoreProperties({ "localizedMessage" })
public abstract class ThrowableMixIn {
    public static final Class<Throwable> HANDLED_TYPE = Throwable.class;
}
