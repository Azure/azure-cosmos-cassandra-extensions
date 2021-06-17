// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing a {@link Statement} object into JSON.
 */
@JsonAppend()
@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(value = { "queryString" }, alphabetic = true)
@JsonIgnoreProperties({ "nowInSeconds", "outgoingPayload", "routingKey" })
public abstract class StatementMixIn {
    public static final Class<Statement> HANDLED_TYPE = Statement.class;
}
