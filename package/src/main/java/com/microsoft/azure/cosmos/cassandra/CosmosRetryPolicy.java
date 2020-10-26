// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
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
 * {@link #growingBackoffTimeInMillis} milliseconds (default: 1000 ms) on each retry, unless maxRetryCount is -1, in
 * which case we back-off with fixed {@link #fixedBackoffTimeInMillis} duration.
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
        return this.maxRetryCount;
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

        // TODO (DANOBLE) ServerError is not the same as ConnectionException and there is no obvious replacement
        //  Consequently this decision tree must be rethought based on the new and very different CoordinatorException
        //  hierarchy
        try {
            if (error instanceof ServerError) {
                return this.retryManyTimesOrThrow(retryCount);
            }
            if (error instanceof OverloadedException || error instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                    int retryAfterMillis = getRetryAfterMillis(error.toString());
                    if (retryAfterMillis == -1) {
                        retryAfterMillis = this.maxRetryCount == -1
                            ? this.fixedBackoffTimeInMillis
                            : this.growingBackoffTimeInMillis * retryCount + random.nextInt(growingBackoffSaltMillis);
                    }
                    Thread.sleep(retryAfterMillis);
                    retryDecision = RetryDecision.RETRY_SAME;
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

    @Override
    public RetryDecision onRequestAborted(@NonNull Request request, @NonNull Throwable error, int retryCount) {
        // TODO (DANOBLE) Implement CosmosRetryPolicy.onRequestAborted
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
    // "com.datastax.driver.core.exceptions.OverloadedException: Queried host (babas.cassandra.cosmos.azure.com/40
    // .65.106.154:10350)
    // was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10,
    // Additional details='
    private static int getRetryAfterMillis(String exceptionString) {

        final String[] tokens = exceptionString.split(",");

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
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;
    }
}
