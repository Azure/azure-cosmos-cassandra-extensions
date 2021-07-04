// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * A mix-in for serializing a {@link Statement} object into JSON.
 */
@JsonAppend()
@JsonPropertyOrder(value = { "queryString", "tracing", "fetchSize", "readTimeoutMillis" }, alphabetic = true)
@JsonIgnoreProperties({ "nowInSeconds", "outgoingPayload", "routingKey", "valueNames" })
@JsonInclude(NON_NULL)
public abstract class StatementMixIn {
    public static final Class<Statement> HANDLED_TYPE = Statement.class;
}
