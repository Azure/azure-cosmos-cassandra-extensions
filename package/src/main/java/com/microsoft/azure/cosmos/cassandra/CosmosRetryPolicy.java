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
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Random;

/**
 * Implements a Cassandra {@link RetryPolicy} with back-offs for {@link OverloadedException} failures.
 * <p>
 * {@link #maxRetryCount} specifies the number of retries that should be attempted. A value of -1 specifies that an
 * indefinite number of retries should be attempted. For {@link #onReadTimeout}, {@link #onWriteTimeout}, and {@link
 * #onUnavailable}, we retry immediately. For onRequestErrors such as OverLoadedError, we try to parse the exception
 * message and use RetryAfterMs field provided from the server as the back-off duration. If RetryAfterMs is not
 * available, we default to exponential growing back-off scheme. In this case the time between retries is increased by
 * {@link #growingBackoffTimeInMillis} milliseconds (default: 1000 ms) on each retry, unless maxRetryCount is -1, in which
 * case we back-off with fixed {@link #fixedBackoffTimeInMillis} duration.
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final int growingBackoffSaltMillis = 2000;
    private static final Random random = new Random();

    private final int fixedBackoffTimeInMillis;
    private final int growingBackoffTimeInMillis;
    private final int maxRetryCount;

    // endregion

    // region Constructors

    public CosmosRetryPolicy(int maxRetryCount) {
        this(maxRetryCount, 5000, 1000);
    }

    public CosmosRetryPolicy(int maxRetryCount, int fixedBackOffTimeMillis, int growingBackoffTimeInMillis) {
        this.maxRetryCount = maxRetryCount;
        this.fixedBackoffTimeInMillis = fixedBackOffTimeMillis;
        this.growingBackoffTimeInMillis = growingBackoffTimeInMillis;
    }

    // endregion

    // region Accessors

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    // endregion

    // region Methods

    @Override
    public void close() {
    }

    @Override
    public RetryDecision onErrorResponse(
        @NonNull Request request,
        @NonNull CoordinatorException error,
        int retryCount) {

        RetryDecision retryDecision;

        try {
            if (error instanceof ConnectionException) {
                return retryManyTimesOrThrow(retryCount);
            }

            if (error instanceof OverloadedException || error instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                    int retryMillis = getRetryAfterMs(error.toString());
                    if (retryMillis == -1) {
                        retryMillis = (this.maxRetryCount == -1)
                            ? this.fixedBackoffTimeInMillis
                            : this.growingBackoffTimeInMillis * retryCount + random.nextInt(growingBackoffSaltMillis);
                    }

                    Thread.sleep(retryMillis);
                    retryDecision = RetryDecision.retry(null);
                } else {
                    retryDecision = RetryDecision.RETHROW;
                }
            } else {
                retryDecision = RetryDecision.RETHROW;
            }

        } catch (InterruptedException exception) {
            retryDecision = RetryDecision.RETHROW;
        }

        return retryDecision;
    }

    @Override
    public RetryDecision onReadTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel consistencyLevel,
        int blockFor,
        int received,
        boolean dataPresent,
        int retryCount) {

        return this.retryManyTimesOrThrow(retryCount);
    }

    // TODO (DANOBLE) Implement CosmosRetryPolicy.onRequestAborted
    @Override
    public RetryDecision onRequestAborted(@NonNull Request request, @NonNull Throwable error, int retryCount) {
        return null;
    }

    @Override
    public RetryDecision onUnavailable(
        @NonNull Request request,
        @NonNull ConsistencyLevel consistencyLevel,
        int required,
        int alive,
        int retryCount) {

        return this.retryManyTimesOrThrow(retryCount);
    }

    @Override
    public RetryDecision onWriteTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel consistencyLevel,
        @NonNull WriteType writeType,
        int blockFor,
        int received,
        int retryCount) {

        return this.retryManyTimesOrThrow(retryCount);
    }

    // Example exceptionString:
    // "com.datastax.driver.core.exceptions.OverloadedException: Queried host (babatsai.cassandra.cosmos.azure.com/40
    // .65.106.154:10350)
    // was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10,
    // Additional details='
    private static int getRetryAfterMs(String exceptionString) {

        final String[] tokens = exceptionString.toString().split(",");

        for (String token : tokens) {

            final String[] kvp = token.split("=");

            if (kvp.length != 2) {
                continue;
            }
            if (kvp[0].trim().equals("RetryAfterMs")) {
                return Integer.parseInt(kvp[1]);
            }
        }

        return -1;
    }

    private RetryDecision retryManyTimesOrThrow(int retryCount) {
        return this.maxRetryCount == -1 || retryCount < this.maxRetryCount
            ? RetryDecision.retry(null)
            : RetryDecision.RETHROW;
    }
}
