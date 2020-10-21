// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;

import java.util.Random;

/**
 * Implements a Cassandra {@link RetryPolicy} with back-offs for {@link OverloadedException} failures.
 * <p>
 * {@link #maxRetryCount} specifies the number of retries that should be attempted. A value of -1 specifies that an
 * indefinite number of retries should be attempted. For {@link #onReadTimeout}, {@link #onWriteTimeout}, and {@link
 * #onUnavailable}, we retry immediately. For onRequestErrors such as OverLoadedError, we try to parse the exception
 * message and use RetryAfterMs field provided from the server as the back-off duration. If RetryAfterMs is not
 * available, we default to exponential growing back-off scheme. In this case the time between retries is increased by
 * {@link #growingBackOffTimeMillis} milliseconds (default: 1000 ms) on each retry, unless maxRetryCount is -1, in which
 * case we back-off with fixed {@link #fixedBackOffTimeMillis} duration.
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    private final static Random random = new Random();
    private final int fixedBackOffTimeMillis;
    private final int growingBackOffSaltMillis = 2000;
    private final int growingBackOffTimeMillis;
    private final int maxRetryCount;

    public CosmosRetryPolicy(int maxRetryCount) {
        this(maxRetryCount, 5000, 1000);
    }

    public CosmosRetryPolicy(int maxRetryCount, int fixedBackOffTimeMillis, int growingBackOffTimeMillis) {
        this.maxRetryCount = maxRetryCount;
        this.fixedBackOffTimeMillis = fixedBackOffTimeMillis;
        this.growingBackOffTimeMillis = growingBackOffTimeMillis;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    @Override
    public void close() {
    }

    @Override
    public void init(Cluster cluster) {
    }

    @Override
    public RetryDecision onReadTimeout(
        Statement statement,
        ConsistencyLevel consistencyLevel,
        int requiredResponses,
        int receivedResponses,
        boolean dataRetrieved,
        int retryNumber) {
        return retryManyTimesOrThrow(retryNumber);
    }

    @Override
    public RetryDecision onRequestError(
        Statement statement,
        ConsistencyLevel consistencyLevel,
        DriverException driverException,
        int retryNumber) {
        RetryDecision retryDecision;

        try {
            if (driverException instanceof ConnectionException) {
                return retryManyTimesOrThrow(retryNumber);
            }

            if (driverException instanceof OverloadedException || driverException instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryNumber < this.maxRetryCount) {
                    int retryMillis = getRetryAfterMs(driverException.toString());
                    if (retryMillis == -1) {
                        retryMillis = (this.maxRetryCount == -1)
                            ? this.fixedBackOffTimeMillis
                            : this.growingBackOffTimeMillis * retryNumber + random.nextInt(growingBackOffSaltMillis);
                    }

                    Thread.sleep(retryMillis);
                    retryDecision = RetryDecision.retry(null);
                } else {
                    retryDecision = RetryDecision.rethrow();
                }
            } else {
                retryDecision = RetryDecision.rethrow();
            }
        } catch (InterruptedException exception) {
            retryDecision = RetryDecision.rethrow();
        }

        return retryDecision;
    }

    @Override
    public RetryDecision onUnavailable(
        Statement statement,
        ConsistencyLevel consistencyLevel,
        int requiredReplica,
        int aliveReplica,
        int retryNumber) {
        return retryManyTimesOrThrow(retryNumber);
    }

    @Override
    public RetryDecision onWriteTimeout(
        Statement statement,
        ConsistencyLevel consistencyLevel,
        WriteType writeType,
        int requiredAcks,
        int receivedAcks,
        int retryNumber) {
        return retryManyTimesOrThrow(retryNumber);
    }

    // Example exceptionString:
    // "com.datastax.driver.core.exceptions.OverloadedException: Queried host (babatsai.cassandra.cosmos.azure.com/40
    // .65.106.154:10350)
    // was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10,
    // Additional details='
    private static int getRetryAfterMs(String exceptionString) {
        String[] tokens = exceptionString.toString().split(",");
        for (String token : tokens) {
            String[] kvp = token.split("=");
            if (kvp.length != 2) {
                continue;
            }
            if (kvp[0].trim().equals("RetryAfterMs")) {
                String value = kvp[1];
                return Integer.parseInt(value);
            }
        }

        return -1;
    }

    private RetryDecision retryManyTimesOrThrow(int retryNumber) {
        return (this.maxRetryCount == -1 || retryNumber < this.maxRetryCount)
            ? RetryDecision.retry(null)
            : RetryDecision.rethrow();
    }
}
