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
import com.datastax.driver.core.policies.RetryPolicy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

import static com.azure.cosmos.cassandra.CosmosJson.toJson;

/**
 * A {@link RetryPolicy} implementation with back-offs for {@link OverloadedException} failures.
 * <p>
 * This is the default retry policy when you take a dependency on {@code azure-cosmos-cassandra-driver-4-extensions}. It
 * provides a good out-of-box experience for communicating with Cosmos Cassandra API instances. Its behavior is
 * specified using these {@link CosmosRetryPolicy.Builder builder} methods:
 * <p><pre>{@code
 *   CosmosRetryPolicy.builder()
 *     .withMaxRetries(5)             // Maximum number of retries.
 *     .withFixedBackOffTime(5_000)   // Fixed backoff time in milliseconds.
 *     .withGrowingBackOffTime(1_000) // Growing backoff time in milliseconds.
 *     .withReadTimeoutRetries(true)  // Whether retries on read timeouts are enabled. Disabling read timeouts may be
 *                                    // desirable when Cosmos Cassandra API server-side retries are enabled.
 *     .withWriteTimeoutRetries(true) // Whether retries on write timeouts are enabled. Disabling write timeouts may be
 *                                    // desirable when Cosmos Cassandra API server-side retries are enabled.
 *   }
 * }</pre></p>
 * <p>
 * The number of retries that should be attempted is specified by {@code max-retries}. A value of {@code -1} indicates
 * that an indefinite number of retries should be attempted.
 * <p>
 * Retry decisions by {@link #onRequestError} may be delayed. Delays are triggered in response to {@link
 * OverloadedException} errors. The delay time is extracted from the error message, or&mdash;if the delay time isn't
 * present in the error message&mdash;computed as follows:
 * <p><pre>{@code
 *   this.maxRetryCount == -1
 *     ? fixedBackOffTimeInMillis
 *     : retryCount * growingBackOffTimeInMillis + saltValue;
 * }</pre></p>
 * <p>
 * There are no other uses for {@code fixed-backoff-time} and {@code growing-backoff-time}. Retry decisions are returned
 * immediately in all other cases. There is no delay in returning from any method but one: {@link #onRequestError} when
 * it receives an {@link OverloadedException} error.
 *
 * @see <a href="../../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/retries/">DataStax Java Driver
 * Retries</a>
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final Logger LOG = LoggerFactory.getLogger(CosmosRetryPolicy.class);

    private static final CosmosRetryPolicy DEFAULT = new CosmosRetryPolicy();

    private static final int DEFAULT_FIXED_BACKOFF_TIME_MILLIS = 5_000;
    private static final int DEFAULT_GROWING_BACK_OFF_TIME_MILLIS = 1_000;
    private static final int DEFAULT_MAX_RETRY_COUNT = 5;
    private static final boolean DEFAULT_READ_TIMEOUT_RETRIES_ENABLED = true;
    private static final boolean DEFAULT_WRITE_TIMEOUT_RETRIES_ENABLED = true;

    private static final int GROWING_BACKOFF_SALT_IN_MILLIS = 1_000;
    private static final Random RANDOM = new Random();

    private final int fixedBackOffTimeMillis;
    private final int growingBackOffTimeMillis;
    private final int maxRetryCount;
    private final boolean readTimeoutRetriesEnabled;
    private final boolean writeTimeoutRetriesEnabled;

    // endregion

    // region Constructors

    private CosmosRetryPolicy(final Builder builder) {
        this.maxRetryCount = builder.maxRetryCount;
        this.fixedBackOffTimeMillis = builder.fixedBackOffTimeInMillis;
        this.growingBackOffTimeMillis = builder.growingBackOffTimeMillis;
        this.readTimeoutRetriesEnabled = builder.readTimeoutRetriesEnabled;
        this.writeTimeoutRetriesEnabled = builder.writeTimeoutRetriesEnabled;
    }

    /**
     * Initializes a newly created {@link CosmosRetryPolicy} object with default settings.
     */
    public CosmosRetryPolicy() {
        this.maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        this.fixedBackOffTimeMillis = DEFAULT_FIXED_BACKOFF_TIME_MILLIS;
        this.growingBackOffTimeMillis = DEFAULT_GROWING_BACK_OFF_TIME_MILLIS;
        this.readTimeoutRetriesEnabled = DEFAULT_READ_TIMEOUT_RETRIES_ENABLED;
        this.writeTimeoutRetriesEnabled = DEFAULT_WRITE_TIMEOUT_RETRIES_ENABLED;
    }

    // endregion

    // region Accessors

    /**
     * Gets the fixed back-off time in milliseconds specified by this {@link CosmosRetryPolicy}.
     *
     * @return the fixed backoff time in milliseconds specified by this {@link CosmosRetryPolicy}.
     */
    public int getFixedBackOffTimeMillis() {
        return this.fixedBackOffTimeMillis;
    }

    /**
     * Gets the growing back-off time in milliseconds specified by this {@link CosmosRetryPolicy}.
     *
     * @return the growing back-off time in milliseconds specified by this {@link CosmosRetryPolicy}.
     */
    public int getGrowingBackOffTimeMillis() {
        return this.growingBackOffTimeMillis;
    }

    /**
     * Gets the maximum retry count specified by this {@link CosmosRetryPolicy}.
     *
     * @return the maximum retry count specified by this {@link CosmosRetryPolicy}.
     */
    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    /**
     * Returns {@code true} if read timeouts are retried by this {@link CosmosRetryPolicy} object.
     *
     * @return {@code true} if read timeouts are retried by this {@link CosmosRetryPolicy} object.
     */
    public boolean isReadTimeoutRetriesEnabled() {
        return this.readTimeoutRetriesEnabled;
    }

    /**
     * Returns {@code true} if write timeouts are retried by this {@link CosmosRetryPolicy} object.
     *
     * @return {@code true} if write timeouts are retried by this {@link CosmosRetryPolicy} object.
     */
    public boolean isWriteTimeoutRetriesEnabled() {
        return this.writeTimeoutRetriesEnabled;
    }

    // endregion

    // region Methods

    /**
     * Gets a newly created {@link Builder builder} object for constructing a {@link CosmosRetryPolicy}.
     *
     * @return a newly created {@link CosmosRetryPolicy} builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Closes the current {@link CosmosRetryPolicy}.
     * <p>
     * No action is performed by this method. It is a noop.
     */
    @Override
    public void close() {
        // nothing to do
    }

    /**
     * Gets a default {@link CosmosRetryPolicy} object.
     *
     * @return a default {@link CosmosRetryPolicy} object
     */
    public static CosmosRetryPolicy defaultPolicy() {
        return DEFAULT;
    }

    /** Initialized the current {@link CosmosRetryPolicy}.
     * <p>
     * No action is perforned by this method. It is a noop.
     */
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
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onReadTimeout("
                    + "statement: {}, "
                    + "consistencyLevel: {}, "
                    + "requiredResponses: {}, "
                    + "receivedResponses: {}, "
                    + "dataRetrieved: {}, "
                    + "retryCount: {})",
                toJson(statement),
                toJson(consistencyLevel),
                toJson(requiredResponses),
                toJson(receivedResponses),
                toJson(dataRetrieved),
                toJson(retryCount));
        }

        final RetryDecision decision;

        if (!this.readTimeoutRetriesEnabled) {
            decision = RetryDecision.rethrow();
        } else {
            decision = receivedResponses >= requiredResponses && !dataRetrieved
                ? RetryDecision.retry(null)
                : RetryDecision.rethrow();
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("onReadTimeout -> returns({}) from {}", toJson(decision), this);
        }

        return decision;
    }

    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "False alarm on Java 11")
    @Override
    public RetryDecision onRequestError(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final DriverException error,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onRequestError(statement: {}, consistencyLevel: {}, error: {}, retryCount: {})",
                toJson(statement),
                toJson(consistencyLevel),
                toJson(error),
                toJson(retryCount));
        }

        RetryDecision decision = RetryDecision.rethrow();

        if (error instanceof ConnectionException) {

            decision = this.retryManyTimesOrThrow(retryCount);

        } else if (error instanceof OverloadedException) {

            if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                int retryAfterMillis = getRetryAfterMillis(error.getMessage());
                if (retryAfterMillis == -1) {
                    retryAfterMillis = this.maxRetryCount == -1
                        ? this.fixedBackOffTimeMillis
                        : retryCount * this.growingBackOffTimeMillis + RANDOM.nextInt(GROWING_BACKOFF_SALT_IN_MILLIS);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("onRequestError -> sleeping on {} for {} before retrying",
                        Thread.currentThread(),
                        Duration.ofMillis(retryAfterMillis));
                }
                try {
                    Thread.sleep(retryAfterMillis);
                    decision = RetryDecision.retry(null);
                } catch (final InterruptedException exception) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("onRequestError -> sleep on thread {} interrupted: {}",
                            Thread.currentThread(),
                            toJson(exception));
                    }
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("onRequestError -> returns({}) from {}", toJson(decision), this);
        }

        return decision;
    }

    @Override
    public RetryDecision onUnavailable(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final int required,
        final int alive,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onUnavailable("
                + "statement: {}, "
                + "consistencyLevel: {}, "
                + "required: {}, "
                + "alive: {}, "
                + "retryCount: {})",
                toJson(statement),
                toJson(consistencyLevel),
                toJson(required),
                toJson(alive),
                toJson(retryCount));
        }

        final RetryDecision decision = this.retryManyTimesOrThrow(retryCount);

        if (LOG.isTraceEnabled()) {
            LOG.trace("onUnavailable -> returns({}) from {}", toJson(decision), this);
        }

        return decision;
    }

    @Override
    public RetryDecision onWriteTimeout(
        final Statement statement,
        final ConsistencyLevel consistencyLevel,
        final WriteType writeType,
        final int required,
        final int received,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onWriteTimeout("
                    + "statement: {}"
                    + "consistencyLevel: {}, "
                    + "writeType: {}, "
                    + "required: {}, "
                    + "received: {}, "
                    + "retryCount: {})",
                toJson(statement),
                toJson(consistencyLevel),
                toJson(writeType),
                toJson(required),
                toJson(received),
                toJson(retryCount));
        }

        if (!this.writeTimeoutRetriesEnabled) {
            return RetryDecision.rethrow();
        }

        final RetryDecision decision = retryCount == 0 && writeType == WriteType.BATCH_LOG
            ? RetryDecision.retry(null)
            : RetryDecision.rethrow();

        if (LOG.isTraceEnabled()) {
            LOG.trace("onWriteTimeout -> returns({}) from {}", toJson(decision), this);
        }

        return decision;
    }

    @Override
    public String toString() {
        return CosmosJson.toString(this);
    }

    // endregion

    // region Privates

    /**
     * Extracts the {@code RetryAfterMs} field from an error message.
     * <p>
     * Here is an example error message:
     * <p><pre>{@code
     * Queried host (babas.cassandra.cosmos.azure.com/40.65.106.154:10350) was overloaded: Request rate is large:
     * ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10, Additional details='
     * }</pre></p>
     *
     * @param errorMessage The error messages.
     *
     * @return Value of the {@code RetryAfterMs} field or {@code -1}, if the field is missing.
     */
    private static int getRetryAfterMillis(final String errorMessage) {

        final String[] tokens = errorMessage.split(",");

        for (final String token : tokens) {

            final String[] kvp = token.split("=");

            if (kvp.length != 2) {
                continue;
            }
            if ("RetryAfterMs".equals(kvp[0].trim())) {
                return Integer.parseInt(kvp[1]);
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

        private int fixedBackOffTimeInMillis = DEFAULT_FIXED_BACKOFF_TIME_MILLIS;
        private int growingBackOffTimeMillis = DEFAULT_GROWING_BACK_OFF_TIME_MILLIS;
        private int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        private boolean readTimeoutRetriesEnabled = DEFAULT_READ_TIMEOUT_RETRIES_ENABLED;
        private boolean writeTimeoutRetriesEnabled = DEFAULT_WRITE_TIMEOUT_RETRIES_ENABLED;

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
         * @return a reference to the current {@link Builder}.
         */
        public Builder withFixedBackOffTimeInMillis(final int value) {
            this.fixedBackOffTimeInMillis = value;
            return this;
        }

        /**
         * Sets the value of the growing back-off time in milliseconds.
         *
         * @param value growing backoff time in milliseconds.
         *
         * @return a reference to the current {@link Builder}.
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
         * @return a reference to the current {@link Builder}.
         */
        public Builder withMaxRetryCount(final int value) {
            this.maxRetryCount = value;
            return this;
        }
        
        /**
         * Sets a value indicating whether read timeouts should be retried.
         *
         * @param enabled {@code true} if read timeouts should be retried.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withReadTimeoutRetries(final boolean enabled) {
            this.readTimeoutRetriesEnabled = enabled;
            return this;
        }

        /**
         * Sets a value indicating whether write timeouts should be retried.
         *
         * @param enabled {@code true} if write timeouts should be retried.
         *
         * @return a reference to the current {@link Builder}.
         */
        public Builder withWriteTimeoutRetries(final boolean enabled) {
            this.writeTimeoutRetriesEnabled = enabled;
            return this;
        }
    }

    // endregion
}
