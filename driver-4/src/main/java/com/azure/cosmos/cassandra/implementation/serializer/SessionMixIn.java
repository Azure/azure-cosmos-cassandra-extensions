// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.session.Session;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing {@link Session} instances to JSON for use in log messages.
 */
@JsonAppend()
@JsonPropertyOrder(value = { "name" }, alphabetic = true)
@JsonIncludeProperties({ "name", "context", "keyspace", "metadata", "metrics" })
public abstract class SessionMixIn {
    public static final Class<Session> HANDLED_TYPE = Session.class;
}
