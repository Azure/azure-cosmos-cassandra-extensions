// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.annotation.JsonAppend.Prop;

/**
 * A mix-in for serializing {@link RequestTracker} instances to JSON for use in log messages.
 */
@JsonAppend(props = @Prop(value = TypePropertyWriter.class, name = "type", type = String.class), prepend = true)
public abstract class RequestTrackerMixIn {
    public static final Class<RequestTracker> HANDLED_TYPE = RequestTracker.class;
}
