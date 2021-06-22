// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

/**
 * A mix-in for serializing a {@link Session} object into JSON.
 */
@JsonAppend()
@JsonIgnoreProperties("state")
@JsonPropertyOrder({ "cluster", "closed", "loggedKeyspace" })
public abstract class SessionMixIn {
    public static final Class<Session> HANDLED_TYPE = Session.class;
}
