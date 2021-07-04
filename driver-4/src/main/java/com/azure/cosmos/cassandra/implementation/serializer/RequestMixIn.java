// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.session.Request;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link Request} instances to JSON for use in log messages.
 */
@JsonAppend()
@JsonPropertyOrder(value = { "query", "namedValues", "positionalValues" }, alphabetic = true)
@JsonIgnoreProperties({ "customPayload", "routingKey" })
public abstract class RequestMixIn {
    public static final Class<Request> HANDLED_TYPE = Request.class;
}
