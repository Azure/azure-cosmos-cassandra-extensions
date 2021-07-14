// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

import static com.azure.cosmos.cassandra.CosmosJson.toJson;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.FIXED_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.GROWING_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.MAX_RETRIES;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.READ_TIMEOUT_RETRIES;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.WRITE_TIMEOUT_RETRIES;

/**
 * A {@link RetryPolicy} implementation with back-offs for {@link OverloadedException} failures.
 * <p>
 * This is the default retry policy when you take a dependency on {@code azure-cosmos-cassandra-driver-4-extensions}. It
 * provides a good out-of-box experience for communicating with Cosmos Cassandra API instances. Its behavior is
 * specified in configuration:
 * <p><pre>{@code
 *   datastax-java-driver.advanced.retry-policy {
 *     max-retries = 5               # Maximum number of retries.
 *     fixed-backoff-time = 5000     # Fixed backoff time in milliseconds.
 *     growing-backoff-time = 1000   # Growing backoff time in milliseconds.
 *     read-timeout-retries = true   # Whether retries on read timeouts are enabled. Disabling read timeouts may be
 *                                   # desirable when Cosmos Cassandra API server-side retries are enabled.
 *     write-timeout-retries = true  # Whether retries on write timeouts are enabled. Disabling write timeouts may be
 *                                   # desirable when Cosmos Cassandra API server-side retries are enabled.
 *   }
 * }</pre></p>
 * <p>
 * The number of retries that should be attempted is specified by {@code max-retries}. A value of {@code -1} indicates
 * that an indefinite number of retries should be attempted.
 * <p>
 * Retry decisions by {@link #onErrorResponse} may be delayed. Delays are triggered in response to {@link
 * OverloadedException} errors. The delay time is extracted from the error message, or&mdash;if the delay time isn't
 * present in the error message&mdash;computed as follows:
 * <p><pre>{@code
 *   this.maxRetryCount == -1
 *     ? fixedBackOffTimeInMillis
 *     : retryCount * growingBackOffTimeInMillis + saltValue;
 * }</pre></p>
 * <p>
 * There are no other uses for {@code fixed-backoff-time} and {@code growing-backoff-time}. Retry decisions are returned
 * immediately in all other cases. There is no delay in returning from any method but one: {@link #onErrorResponse} when
 * it receives an {@link OverloadedException} error.
 *
 * @see <a href="../../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/retries/">DataStax Java Driver
 * Retries</a>
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final Logger LOG = LoggerFactory.getLogger(CosmosRetryPolicy.class);

    private static final int GROWING_BACKOFF_SALT_IN_MILLIS = 1_000;
    private static final Random RANDOM = new Random();

    private final int fixedBackOffTimeInMillis;
    private final int growingBackOffTimeInMillis;
    private final int maxRetryCount;
    private final boolean readTimeoutRetriesEnabled;
    private final boolean writeTimeoutRetriesEnabled;

    // endregion

    // region Constructors

    /**
     * Initializes a newly created {@link CosmosRetryPolicy} object from configuration.
     *
     * @param driverContext an object holding the context of the current driver instance.
     * @param profileName   name of the configuration profile to apply.
     */
    @SuppressWarnings("unused")
    public CosmosRetryPolicy(final DriverContext driverContext, final String profileName) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("CosmosRetryPolicy(sessionName: {}, profileName: {})",
                toJson(driverContext.getSessionName()),
                toJson(profileName));
        }

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.maxRetryCount = MAX_RETRIES.getValue(profile, Integer.class);
        this.fixedBackOffTimeInMillis = FIXED_BACKOFF_TIME.getValue(profile, Integer.class);
        this.growingBackOffTimeInMillis = GROWING_BACKOFF_TIME.getValue(profile, Integer.class);
        this.readTimeoutRetriesEnabled = READ_TIMEOUT_RETRIES.getValue(profile, Boolean.class);
        this.writeTimeoutRetriesEnabled = WRITE_TIMEOUT_RETRIES.getValue(profile, Boolean.class);

        if (LOG.isDebugEnabled()) {
            LOG.debug("CosmosRetryPolicy -> {}", toJson(this));
        }
    }

    CosmosRetryPolicy(final int maxRetryCount) {
        this(
            maxRetryCount,
            FIXED_BACKOFF_TIME.getDefaultValue(Integer.class),
            GROWING_BACKOFF_TIME.getDefaultValue(Integer.class),
            READ_TIMEOUT_RETRIES.getDefaultValue(Boolean.class),
            WRITE_TIMEOUT_RETRIES.getDefaultValue(Boolean.class));
    }

    CosmosRetryPolicy(
        final int maxRetryCount,
        final int fixedBackOffTimeInMillis,
        final int growingBackoffTimeInMillis,
        final boolean readTimeoutRetriesEnabled,
        final boolean writeTimeoutRetriesEnabled) {

        this.maxRetryCount = maxRetryCount;
        this.fixedBackOffTimeInMillis = fixedBackOffTimeInMillis;
        this.growingBackOffTimeInMillis = growingBackoffTimeInMillis;
        this.readTimeoutRetriesEnabled = readTimeoutRetriesEnabled;
        this.writeTimeoutRetriesEnabled = writeTimeoutRetriesEnabled;
    }

    // endregion

    // region Accessors

    /**
     * Gets the {@code fixed-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     *
     * @return the {@code fixed-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     */
    public int getFixedBackOffTimeInMillis() {
        return this.fixedBackOffTimeInMillis;
    }

    /**
     * Gets the {@code growing-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     *
     * @return the {@code growing-backoff-time} specified by this {@link CosmosRetryPolicy} object.
     */
    public int getGrowingBackOffTimeInMillis() {
        return this.growingBackOffTimeInMillis;
    }

    /**
     * Gets the {@code max-retries} value specified by this {@link CosmosRetryPolicy} object.
     * <p>
     * A value of {@code -1} indicates that an indefinite number of retries should be attempted.
     *
     * @return the {@code max-retries} value specified by this {@link CosmosRetryPolicy} object.
     */
    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    /**
     * Gets the {@code retry-read-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     * <p>
     * This value indicates whether read timeouts are enabled.
     *
     * @return The {@code retry-read-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     */
    public boolean isReadTimeoutRetriesEnabled() {
        return this.readTimeoutRetriesEnabled;
    }

    /**
     * Gets the {@code retry-write-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     * <p>
     * This value indicates whether write timeouts are enabled.
     *
     * @return The {@code retry-write-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     */
    public boolean isWriteTimeoutRetriesEnabled() {
        return this.writeTimeoutRetriesEnabled;
    }

    /**
     * Closes the current {@link CosmosRetryPolicy}.
     */
    @Override
    public void close() {
        // nothing to do
    }

    /**
     * Returns a retry decision in response to a recoverable error other than READ_TIMEOUT, WRITE_TIMEOUT or
     * UNAVAILABLE.
     * <p>
     * Recoverable errors are represented by these exception types:
     * <p><ul>
     * <li>{@link OverloadedException},</li>
     * <li>{@link ServerError},</li>
     * <li>{@link TruncateException},</li>
     * <li>{@link ReadFailureException}, and</li>
     * <li>{@link WriteFailureException}.</li>
     * </ul><p>
     * The following errors are handled internally by the driver, and therefore will never be encountered by this
     * method:
     * <p><ul>
     * <li>{@link BootstrappingException} is always retried on the next node;</li>
     * <li>{@link QueryValidationException} (and its subclasses),</li>
     * <li>{@link FunctionFailureException}, and</li>
     * <li>{@link ProtocolError}: always rethrown.</li>
     * </ul></p>
     * <b>Note:</b> This method will only be invoked for {@link Request#isIdempotent idempotent} requests. When
     * execution was aborted before getting a response, it is impossible to determine with 100% certainty whether a
     * mutation was applied or not, so a write is never safe to retry; the driver will rethrow the error directly,
     * without invoking the retry policy.
     *
     * @param request    The request that failed.
     * @param error      The error received.
     * @param retryCount The current retry count.
     *
     * @return A {@link RetryDecision} based on {@code error} and {@code retryCount}:
     * <ul>
     * <li>{@link RetryDecision#RETRY_SAME Retry on the same node}, if an {@link OverloadedException} or
     * {@link WriteFailureException} is received and the {@code retryCount} is less than {@link #getMaxRetryCount};
     * <li>{@link RetryDecision#RETHROW} otherwise.</li>
     * </ul><p>
     * When retrying, this method pauses for the duration of the retry interval extracted from
     * {@link CoordinatorException#getMessage error message} or--if there is no retry interval in the error
     * message--as computed from the {@link CosmosRetryPolicyOption options} specified for this {@link RetryPolicy}. If
     * {@link #getMaxRetryCount}
     * <p><pre>{@code
     *     retryAfterMillis = this.getFixedBackOffTimeInMillis()
     *         + retryCount * this.getGrowingBackOffTime()
     *         + RANDOM.nextInt(GROWING_BACKOFF_SALT_IN_MILLIS)
     * }</pre><p>
     * Or, if {@link #getMaxRetryCount} is {@code -1}:
     * <p><pre>{@code
     *     this.getFixedBackOffTimeInMillis();
     * }</pre>
     */
    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "False alarm")
    @Override
    public RetryDecision onErrorResponse(
        @NonNull final Request request,
        @NonNull final CoordinatorException error,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onErrorResponse(request: {}, error: {}, retryCount: {})",
                toJson(request),
                toJson(error),
                toJson(retryCount));
        }

        RetryDecision retryDecision = RetryDecision.RETHROW;

        if (error instanceof OverloadedException) {
            if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                int retryAfterMillis = getRetryAfterMillis(error.getMessage());
                if (retryAfterMillis == -1) {
                    retryAfterMillis = this.maxRetryCount == -1
                        ? this.fixedBackOffTimeInMillis
                        : retryCount * this.growingBackOffTimeInMillis + RANDOM.nextInt(GROWING_BACKOFF_SALT_IN_MILLIS);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("--> sleeping on {} for {} before retrying",
                        Thread.currentThread(),
                        Duration.ofMillis(retryAfterMillis));
                }
                try {
                    Thread.sleep(retryAfterMillis);
                    retryDecision = RetryDecision.RETRY_SAME;
                } catch (final InterruptedException exception) {
                    LOG.trace("--> sleep on thread {} interrupted: ", Thread.currentThread(), exception);
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("--> returning {}", retryDecision);
        }

        return retryDecision;
    }

    /**
     * Returns a {@link RetryDecision#RETRY_SAME retry-same} or {@link RetryDecision#RETHROW} decision in response to
     * a {@link ReadTimeoutException read timeout error}.
     * <p>
     * Retries on read timeouts can be disabled by setting {@link CosmosRetryPolicyOption#READ_TIMEOUT_RETRIES} to
     * {@code false}:
     * <p><pre>{@code
     *     datastax-java-driver.advanced.retry-policy.retry-write-timeouts = false
     * }</pre></p>
     * Disabling retries for read timeouts may be desirable when Cosmos DB server-side retries are enabled.
     *
     * @param request          The request that failed.
     * @param consistencyLevel The consistency level of the request that failed.
     * @param blockFor         The minimum number of replica acknowledgements/responses that were required to fulfill
     *                         the operation.
     * @param received         The number of replica acknowledgements/responses to the operation before it failed.
     * @param dataPresent      Whether the actual data was amongst the received replica responses. See {@link
     *                         ReadTimeoutException#wasDataPresent}.
     * @param retryCount       The current retry count.
     *
     * @return A maximum of one {@link RetryDecision#RETRY_SAME retry to the same node}, if the number of replica
     * acknowledgements/responses received before the operation failed (the value of {@code received}) is greater than
     * or equal to the minimum number of replica acknowledgements/responses required to fulfill the operation (the value
     * of {@code blockFor}), data was not retrieved ({@code dataPresent}: false}, and only if retries on read timeouts
     * are enabled (the default). Otherwise, a {@link RetryDecision#RETHROW rethrow decision} is returned.
     */
    @Override
    public RetryDecision onReadTimeout(
        @NonNull final Request request,
        @NonNull final ConsistencyLevel consistencyLevel,
        final int blockFor,
        final int received,
        final boolean dataPresent,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onReadTimeout("
                    + "request: {}, "
                    + "consistencyLevel: {}, "
                    + "blockFor: {}, "
                    + "received: {}, "
                    + "dataPresent: {}, "
                    + "retryCount: {})",
                toJson(request),
                toJson(consistencyLevel),
                toJson(blockFor),
                toJson(received),
                toJson(dataPresent),
                toJson(retryCount));
        }

        final RetryDecision decision;

        if (!this.readTimeoutRetriesEnabled) {
            decision = RetryDecision.RETHROW;
        } else {
            decision = retryCount == 0 && received >= blockFor && !dataPresent
                ? RetryDecision.RETRY_SAME
                : RetryDecision.RETHROW;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("--> returning {}", toJson(decision));
        }

        return decision;
    }

    /**
     * Returns a {@link RetryDecision#RETRY_SAME retry-same} or {@link RetryDecision#RETHROW} decision when a request is
     * aborted before a response is received.
     * <p>
     * This can happen in two cases:
     * <p><ul>
     * <li>The connection was closed due to an external event. This will manifest as a {@link ClosedConnectionException}
     * (network failure) or a {@link HeartbeatException} (missed heartbeat); or</li>
     * <li>There was an unexpected error while decoding the response. This can only be a driver bug.</li>
     * </ul><p>
     * This method is only invoked for idempotent statements.
     *
     * @param request    The request that failed.
     * @param error      The error received.
     * @param retryCount The current retry count.
     *
     * @return A {@link RetryDecision#RETRY_SAME retry-same decision}, if a {@link ClosedConnectionException} or
     * {@link HeartbeatException} is received and {@code retryCount} is less than {@link #getMaxRetryCount}; otherwise,
     * a {@link RetryDecision#RETHROW rethrow} decision.
     */
    @Override
    public RetryDecision onRequestAborted(
        @NonNull final Request request, @NonNull final Throwable error, final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onRequestAborted(request: {}, error: {}, retryCount: {})",
                toJson(request),
                toJson(error),
                toJson(retryCount));
        }

        final RetryDecision decision = error instanceof ClosedConnectionException || error instanceof HeartbeatException
            ? this.retryManyTimesOrThrow(retryCount)
            : RetryDecision.RETHROW;

        if (LOG.isTraceEnabled()) {
            LOG.trace("--> returning {}", toJson(decision));
        }

        return decision;
    }

    /**
     * Returns a {@link RetryDecision#RETRY_SAME retry-same} or {@link RetryDecision#RETHROW} decision in response to an
     * {@code UNAVAILABLE} error.
     *
     * @param request          The request that failed.
     * @param consistencyLevel The consistency level of the request that failed.
     * @param required         The number of replica acknowledgements/responses required to perform the operation given
     *                         its {@code consistencyLevel}.
     * @param alive            The number of replicas that were known to be alive by the coordinator node when it tried
     *                         to execute the operation.
     * @param retryCount       The current retry count.
     *
     * @return A {@link RetryDecision#RETRY_SAME retry-same decision}, if {@code retryCount} is less than the
     * {@link #getMaxRetryCount}.
     */
    @Override
    public RetryDecision onUnavailable(
        @NonNull final Request request,
        @NonNull final ConsistencyLevel consistencyLevel,
        final int required,
        final int alive,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onUnavailable("
                    + "request: {}, "
                    + "consistencyLevel: {}, "
                    + "required: {}, "
                    + "alive: {}, "
                    + "retryCount: {})",
                toJson(request),
                toJson(consistencyLevel),
                toJson(required),
                toJson(alive),
                toJson(retryCount));
        }

        final RetryDecision decision = this.retryManyTimesOrThrow(retryCount);

        if (LOG.isTraceEnabled()) {
            LOG.trace("--> returning {}", decision);
        }

        return decision;
    }

    /**
     * Returns a {@link RetryDecision#RETRY_SAME retry-same} or {@link RetryDecision#RETHROW rethrow} decision in
     * response to a {@link WriteTimeoutException write timeout error}.
     * <p>
     * This method is only invoked for idempotent statements. It is similar to {link #onReadTimeout}, but for write
     * operations.
     * <p>
     * Retries on write timeouts can be disabled by setting {@link CosmosRetryPolicyOption#WRITE_TIMEOUT_RETRIES} to
     * {@code false}:
     * <p><pre>{@code
     *     datastax-java-driver.advanced.retry-policy.retry-write-timeouts = false
     * }</pre></p>
     * Disabling retries for write timeouts may be desirable when Cosmos DB server-side retries are enabled.
     *
     * @param request          The request that failed.
     * @param consistencyLevel The consistency level of the request that failed.
     * @param writeType        The kind of write query.
     * @param blockFor         The minimum number of replica acknowledgements/responses that were required to fulfill
     *                         the operation.
     * @param received         The number of replica acknowledgements/responses to the operation before it failed.
     * @param retryCount       The current retry count.
     *
     * @return A maximum of one {@link RetryDecision#RETRY_SAME retry to the same node}, and only for
     * {@link WriteType#BATCH_LOG batch log} operations, if and only if retries on write-timeouts are enabled (the
     * default). Otherwise, a {@link RetryDecision#RETHROW rethrow decision} is returned.
     */
    @Override
    public RetryDecision onWriteTimeout(
        @NonNull final Request request,
        @NonNull final ConsistencyLevel consistencyLevel,
        @NonNull final WriteType writeType,
        final int blockFor,
        final int received,
        final int retryCount) {

        if (LOG.isTraceEnabled()) {
            LOG.trace("onWriteTimeout("
                    + "request: {}"
                    + "consistencyLevel: {}, "
                    + "writeType: {}, "
                    + "blockFor: {}, "
                    + "received: {}, "
                    + "retryCount: {})",
                request,
                consistencyLevel,
                writeType,
                blockFor,
                received,
                retryCount);
        }

        if (!this.writeTimeoutRetriesEnabled) {
            return RetryDecision.RETHROW;
        }

        final RetryDecision decision = retryCount == 0 && writeType == DefaultWriteType.BATCH_LOG
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

        if (LOG.isTraceEnabled()) {
            LOG.trace("--> returning {}", toJson(decision));
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
     *   ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10, Additional details='
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

    private RetryDecision retryManyTimesOrThrow(final int retryCount) {
        return this.maxRetryCount == -1 || retryCount < this.maxRetryCount
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;
    }

    // endregion
}
