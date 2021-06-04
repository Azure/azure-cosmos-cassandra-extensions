// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.policies.RetryPolicy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Random;

/**
 * Implements a {@link RetryPolicy} with back-offs for {@link OverloadedException} failures.
 * <p>
 * {@code maxRetryCount} specifies the number of retries that should be attempted. A value of -1 specifies that an
 * indefinite number of retries should be attempted. For {@link #onReadTimeout}, {@link #onWriteTimeout}, and
 * {@link #onUnavailable}, we retry immediately. For an {@link #onRequestError} such as {@link OverloadedException}, we
 * try to parse the exception message and use {@code RetryAfterMs} field provided from the server as the back-off
 * duration. If {@code RetryAfterMs} is not available, we default to exponential growing back-off scheme. In this case
 * the time between retries is increased by {@code growingBackOffTimeMillis} milliseconds (default: {@code 1000 ms}) on
 * each retry, unless {@code maxRetryCount} is {@code -1}, in which case we back-off with
 * {@link #fixedBackOffTimeMillis} duration.
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final Random RANDOM = new Random();
    private final int fixedBackOffTimeMillis;
    private final int growingBackOffTimeMillis;
    private final int maxRetryCount;

    // endregion

    // region Constructors

    private CosmosRetryPolicy(final Builder builder) {
        this.maxRetryCount = builder.maxRetryCount;
        this.fixedBackOffTimeMillis = builder.fixedBackOffTimeInMillis;
        this.growingBackOffTimeMillis = builder.growingBackOffTimeMillis;
    }

    /**
     * Initializes a newly created {@link CosmosRetryPolicy} object
     *
     * @param maxRetryCount maximum number of times to retry an operation before failing.
     *
     * @deprecated use {@link #builder CosmosRetryPolicy.builder} to construct a CosmosRetryPolicy instead.
     */
    @Deprecated
    public CosmosRetryPolicy(final int maxRetryCount) {
        this(maxRetryCount, 5000, 1000);
    }

    /**
     * Initializes a newly created {@link CosmosRetryPolicy} object
     *
     * @param maxRetryCount maximum number of times to retry an operation before failing.
     * @param fixedBackOffTimeMillis fixed backoff time in milliseconds.
     * @param growingBackOffTimeMillis growing backoff time in milliseconds.
     *
     * @deprecated use {@link #builder CosmosRetryPolicy.builder} to construct a CosmosRetryPolicy instead.
     */
    @Deprecated
    public CosmosRetryPolicy(
        final int maxRetryCount, final int fixedBackOffTimeMillis, final int growingBackOffTimeMillis) {

        this.maxRetryCount = maxRetryCount;
        this.fixedBackOffTimeMillis = fixedBackOffTimeMillis;
        this.growingBackOffTimeMillis = growingBackOffTimeMillis;
    }

    // endregion

    // region Accessors

    /**
     * Gets a newly created {@link Builder builder} object for constructing a {@link CosmosRetryPolicy}.
     *
     * @return a newly created {@link CosmosLoadBalancingPolicy} builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the maximum retry count specified by this {@link CosmosLoadBalancingPolicy}.
     *
     * @return the maximum retry count specified by this {@link CosmosLoadBalancingPolicy}.
     */
    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    // endregion

    // region Methods

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void init(final Cluster cluster) {
        // nothing to do
    }

    @Override
    public RetryDecision onReadTimeout(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final int requiredResponses,
        final int receivedResponses,
        final boolean dataRetrieved,
        final int retryNumber) {
        return this.retryManyTimesOrThrow(retryNumber);
    }

    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "False alarm on Java 11")
    @Override
    public RetryDecision onRequestError(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final DriverException driverException,
        final int retryNumber) {

        RetryDecision retryDecision;

        try {
            if (driverException instanceof ConnectionException) {
                return this.retryManyTimesOrThrow(retryNumber);
            }
            if (driverException instanceof OverloadedException || driverException instanceof WriteFailureException) {
                if (this.maxRetryCount == -1 || retryNumber < this.maxRetryCount) {
                    int retryMillis = getRetryAfterMs(driverException.toString());
                    if (retryMillis == -1) {
                        final int growingBackOffSaltMillis = 2000;
                        retryMillis = this.maxRetryCount == -1
                            ? this.fixedBackOffTimeMillis
                            : this.growingBackOffTimeMillis * retryNumber + RANDOM.nextInt(growingBackOffSaltMillis);
                    }
                    Thread.sleep(retryMillis);
                    retryDecision = RetryDecision.retry(null);
                } else {
                    retryDecision = RetryDecision.rethrow();
                }
            } else {
                retryDecision = RetryDecision.rethrow();
            }
        } catch (final InterruptedException exception) {
            retryDecision = RetryDecision.rethrow();
        }

        return retryDecision;
    }

    @Override
    public RetryDecision onUnavailable(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final int requiredReplica,
        final int aliveReplica,
        final int retryNumber) {
        return this.retryManyTimesOrThrow(retryNumber);
    }

    @Override
    public RetryDecision onWriteTimeout(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final WriteType writeType,
        final int requiredAcks,
        final int receivedAcks,
        final int retryNumber) {
        return this.retryManyTimesOrThrow(retryNumber);
    }

    // endregion

    // region Privtes

    private static int getRetryAfterMs(final String exceptionString) {
        // Example exceptionString:
        // com.datastax.driver.core.exceptions.OverloadedException: Queried host (babatsai.cassandra.cosmos.azure.com/
        // 40.65.106.154:10350) was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f,
        // RetryAfterMs=10, Additional details=
        final String[] tokens = exceptionString.split(",");
        for (final String token : tokens) {
            final String[] kvp = token.split("=");
            if (kvp.length != 2) {
                continue;
            }
            if ("RetryAfterMs".equals(kvp[0].trim())) {
                final String value = kvp[1];
                return Integer.parseInt(value);
            }
        }
        return -1;
    }

    private RetryDecision retryManyTimesOrThrow(final int retryNumber) {
        return (this.maxRetryCount == -1 || retryNumber < this.maxRetryCount)
            ? RetryDecision.retry(null)
            : RetryDecision.rethrow();
    }

    // endregion

    // region Types

    /**
     * A builder for constructing {@link CosmosRetryPolicy} objects.
     */
    public static final class Builder {

        private int fixedBackOffTimeInMillis = 5_000;
        private int growingBackOffTimeMillis = 1_000;
        private int maxRetryCount = 5;

        /**
         * Constructs a new {@link CosmosRetryPolicy} object.
         *
         * @return a newly constructed {@link CosmosRetryPolicy} object.
         */
        public CosmosRetryPolicy build() {
            return new CosmosRetryPolicy(this);
        }

        /**
         * Sets the value of the fixed backoff time in milliseconds.
         *
         * @param value fixed backoff time in milliseconds.
         *
         * @return a reference to the current {@link CosmosLoadBalancingPolicy.Builder}.
         */
        public Builder withFixedBackOffTimeInMillis(final int value) {
            this.fixedBackOffTimeInMillis = value;
            return this;
        }

        /**
         * Sets the value of the growing backoff time in milliseconds.
         *
         * @param value growing backoff time in milliseconds.
         *
         * @return a reference to the current {@link CosmosLoadBalancingPolicy.Builder}.
         */
        public Builder withGrowingBackOffTimeInMillis(final int value) {
            this.growingBackOffTimeMillis = value;
            return this;
        }

        /**
         * Sets the value of the maximum retry count.
         *
         * @param value maximum retry count.
         *
         * @return a reference to the current {@link CosmosLoadBalancingPolicy.Builder}.
         */
        public Builder withMaxRetryCount(final int value) {
            this.maxRetryCount = value;
            return this;
        }
    }

    // endregion
}
