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
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Implements a Cassandra {@link RetryPolicy} with back-offs for {@link OverloadedException} failures
 * <p>
 * Growing/fixed back-offs are performed based on the value of {@link #maxRetryCount}. A value of -1 specifies that
 * an indefinite number of retries should be attempted every {@link #fixedBackOffTimeMs} milliseconds (default:
 * 5000 ms). A value greater than zero specifies that {@link #maxRetryCount} retries should be attempted following a
 * growing back-off scheme. In this case the time between retries is increased by {@link #growingBackOffTimeMs}
 * milliseconds (default: 1000 ms) on each retry.
 * </p>
 */
public class CosmosRetryPolicy implements RetryPolicy {

    public CosmosRetryPolicy(int maxRetryCount) {
        this(maxRetryCount, 5000, 1000);
    }

    public CosmosRetryPolicy(int maxRetryCount, int fixedBackOffTimeMs, int growingBackOffTimeMs) {
        this.maxRetryCount = maxRetryCount;
        this.fixedBackOffTimeMs = fixedBackOffTimeMs;
        this.growingBackOffTimeMs = growingBackOffTimeMs;
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
            if (driverException instanceof OverloadedException) {
                retryDecision = retryManyTimesWithBackOffOrThrow(retryNumber);
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

    private final int fixedBackOffTimeMs;
    private final int growingBackOffTimeMs;
    private final int maxRetryCount;

    private RetryDecision retryManyTimesOrThrow(int retryNumber) {

        RetryDecision retryDecision;

        if (this.maxRetryCount == -1) {
            retryDecision = RetryDecision.retry(null);
        } else {
            if (retryNumber < this.maxRetryCount) {
                retryDecision = RetryDecision.retry(null);
            } else {
                retryDecision = RetryDecision.rethrow();
            }
        }

        return retryDecision;
    }

    private RetryDecision retryManyTimesWithBackOffOrThrow(int retryNumber) throws InterruptedException {

        RetryDecision retryDecision = null;

        if (this.maxRetryCount == -1) {
            Thread.sleep(this.fixedBackOffTimeMs);
            retryDecision = RetryDecision.retry(null);
        } else {
            if (retryNumber < this.maxRetryCount) {
                Thread.sleep(this.growingBackOffTimeMs * retryNumber);
                retryDecision = RetryDecision.retry(null);
            } else {
                retryDecision = RetryDecision.rethrow();
            }
        }

        return retryDecision;
    }
}
