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
 * <p>
 * When {@link #useRetryMillisIfAvailable} is enabled, we try to parse the exception message and use RetryAfterMs
 * field for the back-off duration. If RetryAfterMs is not available, we default to exponential backoff.
 * Growing/fixed back-offs are performed based on the value of {@link #maxRetryCount}. A value of -1 specifies that
 * an indefinite number of retries should be attempted every {@link #fixedBackOffTimeMillis} milliseconds (default:
 * 5000 Millis). A value greater than zero specifies that {@link #maxRetryCount} retries should be attempted following a
 * growing back-off scheme. In this case the time between retries is increased by {@link #growingBackOffTimeMillis}
 * milliseconds (default: 1000 ms) on each retry.
 * </p>
 */
public class CosmosRetryPolicy implements RetryPolicy {

    public CosmosRetryPolicy(int maxRetryCount) {
        this(maxRetryCount, 5000, 1000, true);
    }

    public CosmosRetryPolicy(int maxRetryCount, int fixedBackOffTimeMillis, int growingBackOffTimeMillis, boolean useRetryMillisIfAvailable) {
        this.maxRetryCount = maxRetryCount;
        this.fixedBackOffTimeMillis = fixedBackOffTimeMillis;
        this.growingBackOffTimeMillis = growingBackOffTimeMillis;
        this.useRetryMillisIfAvailable = useRetryMillisIfAvailable;
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
                retryDecision = this.useRetryMillisIfAvailable ?
                        retryManyTimesWithMsInfoOrThrow(retryNumber, driverException.toString()):
                        retryManyTimesWithBackOffOrThrow(retryNumber);
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
    private boolean useRetryMillisIfAvailable;

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
            Thread.sleep(this.fixedBackOffTimeMillis);
            retryDecision = RetryDecision.retry(null);
        } else {
            if (retryNumber < this.maxRetryCount) {
                Thread.sleep(this.growingBackOffTimeMillis * retryNumber + random.nextInt(growingBackOffSaltMillis));
                retryDecision = RetryDecision.retry(null);
            } else {
                retryDecision = RetryDecision.rethrow();
            }
        }

        return retryDecision;
    }

    private RetryDecision retryManyTimesWithMsInfoOrThrow(int retryNumber, String exception) throws InterruptedException {

        RetryDecision retryDecision = null;

        int retryWaitTime = getRetryAfterMs(exception);

        if (this.maxRetryCount == -1) {
            Thread.sleep(retryWaitTime);
            retryDecision = RetryDecision.retry(null);
        } else {
            System.out.println("retrying after "+retryWaitTime+" millseconds");
            System.out.println("retryNumber "+retryNumber+" of "+this.maxRetryCount);
            if (retryNumber < this.maxRetryCount) {
                Thread.sleep(retryWaitTime);
                retryDecision = RetryDecision.retry(null);
            } else {
                retryDecision = RetryDecision.rethrow();
            }
        }

        return retryDecision;
    }

    public int getRetryAfterMs(String exceptionString){
        // TODO: What if the exceptionString is not as we expected?
        //parse the exception test to get retry milliseconds
        int millseconds = 0;
        String[] exceptions = exceptionString.toString().split(",");
        String[] retryProperty = exceptions[1].toString().split("=");
        exceptionString = retryProperty[0].toString().trim();
        if (exceptionString.equals("RetryAfterMs")){
            String value = retryProperty[1];
            millseconds = Integer.parseInt(value);
        }

        return millseconds;
    }
}