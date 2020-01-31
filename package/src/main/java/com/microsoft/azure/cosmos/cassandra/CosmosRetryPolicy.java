/*
 * The MIT License (MIT)
 *
 * Copyright (c) Microsoft. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.policies.RetryPolicy;

import java.util.Random;

/**
 * Implements a Cassandra {@link RetryPolicy} with back-offs for {@link OverloadedException} failures
 * <p> {@link #maxRetryCount} specifies the number of retries should be attempted. A value of -1 specifies that an
 * indefinite number of retries should be attempted. For readTimeout, writeTimeout, and onUnavailable, we retry
 * immediately. For onRequestErrors such as OverLoadedError, we try to parse the exception message and use RetryAfterMs
 * field provided from the server as the back-off duration. If RetryAfterMs is not available, we default to exponential
 * growing back-off scheme. In this case the time between retries is increased by {@link #growingBackOffTimeMillis}
 * milliseconds (default: 1000 ms) on each retry, unless maxRetryCount is -1, in which case we back-off with fixed
 * {@link #fixedBackOffTimeMillis} duration.
 * </p>
 */
public class CosmosRetryPolicy implements RetryPolicy {

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
            if (driverException instanceof OverloadedException || driverException instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryNumber < this.maxRetryCount) {
                    int retryMillis = getRetryAfterMs(driverException.toString());
                    if (retryMillis == -1) {
                        retryMillis = (this.maxRetryCount == -1) ?
                                this.fixedBackOffTimeMillis :
                                this.growingBackOffTimeMillis * retryNumber + random.nextInt(growingBackOffSaltMillis);
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

    private final static Random random = new Random();
    private final int growingBackOffSaltMillis = 2000;
    private final int fixedBackOffTimeMillis;
    private final int growingBackOffTimeMillis;
    private final int maxRetryCount;

    private RetryDecision retryManyTimesOrThrow(int retryNumber) {
        return (this.maxRetryCount == -1 || retryNumber < this.maxRetryCount) ?
                RetryDecision.retry(null) : RetryDecision.rethrow();
    }

    // Example exceptionString:
    // "com.datastax.driver.core.exceptions.OverloadedException: Queried host (babatsai.cassandra.cosmos.azure.com/40.65.106.154:10350)
    // was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10, Additional details='
    public int getRetryAfterMs(String exceptionString){
        String[] tokens = exceptionString.toString().split(",");
        for (String token: tokens) {
            String[] kvp = token.split("=");
            if (kvp.length != 2) continue;
            if (kvp[0].trim().equals("RetryAfterMs")) {
                String value = kvp[1];
                return Integer.parseInt(value);
            }
        }

        return -1;
    }
}