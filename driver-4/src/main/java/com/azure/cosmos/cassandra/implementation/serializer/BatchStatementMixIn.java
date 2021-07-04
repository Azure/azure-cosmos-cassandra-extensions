// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.implementation.serializer;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonAppend;

import java.util.Iterator;

/**
 * A mix-in for serializing {@link BatchStatement} instances to JSON for use in log messages.
 */
@JsonAppend()
@JsonPropertyOrder(value = { "batchType", "statements" }, alphabetic = true)
public abstract class BatchStatementMixIn extends RequestMixIn {
    public static final Class<BatchStatement> HANDLED_TYPE = BatchStatement.class;

    /**
     * Specifies that {@link BatchStatement#iterator()} should be represented as a list-valued property named
     * {@code "statements"}.
     *
     * @return An iterator over the set of statements in this {@link BatchStatement}.
     */
    @JsonProperty("statements")
    public abstract Iterator<BatchableStatement<?>> iterator();
}
