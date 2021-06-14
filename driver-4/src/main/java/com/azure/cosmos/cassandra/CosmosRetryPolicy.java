// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.implementation.Json;
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

import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.FIXED_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.GROWING_BACKOFF_TIME;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.MAX_RETRIES;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.RETRY_READ_TIMEOUTS;
import static com.azure.cosmos.cassandra.CosmosRetryPolicyOption.RETRY_WRITE_TIMEOUTS;
import static com.azure.cosmos.cassandra.implementation.Json.toJson;

/**
 * A {@link RetryPolicy} implementation with back-offs for {@link CoordinatorException} failures.
 * <p>
 * This is the default retry policy when you take a dependency on this package. It provides a good out-of-box experience
 * for communicating with Cosmos Cassandra API instances. Its behavior is specified in configuration:
 * <pre>{@code
 * datastax-java-driver.advanced.retry-policy {
 *   class = DefaultRetryPolicy
 *   max-retries = 5              # Maximum number of retries
 *   fixed-backoff-time = 5000    # Fixed backoff time in milliseconds
 *   growing-backoff-time = 1000  # Growing backoff time in milliseconds
 * }
 * }</pre>
 * The number of retries that should be attempted is specified by {@code max-retries}. A value of {@code -1} specifies
 * that an indefinite number of retries should be attempted. For {@link #onReadTimeout onReadTimout}, {@link
 * #onWriteTimeout onWriteTimeout}, and {@link #onUnavailable onUnavailable}, retries are immediate. For {@link
 * #onErrorResponse onResponse}, the exception message is parsed to obtain the value of the {@code RetryAfterMs} field
 * provided by the server as the back-off duration. If {@code RetryAfterMs} is not available, the default exponential
 * growing back-off scheme is used. In this case the time between retries is increased by {@code growing-backoff-time}
 * on each retry, unless {@code max-retries} is {@code -1}. In this case back-off occurs with fixed {@code
 * fixed-backoff-time} duration.
 *
 * @see <a href="../../../../../doc-files/reference.conf.html">reference.conf</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries/">DataStax Java
 * Driver Retries</a>
 */
public final class CosmosRetryPolicy implements RetryPolicy {

    // region Fields

    private static final int GROWING_BACKOFF_SALT_IN_MILLIS = 1_000;
    private static final Logger LOG = LoggerFactory.getLogger(CosmosRetryPolicy.class);
    private static final Random RANDOM = new Random();

    private final int fixedBackOffTimeInMillis;
    private final int growingBackOffTimeInMillis;
    private final int maxRetryCount;
    private final boolean retryReadTimeoutsEnabled;
    private final boolean retryWriteTimeoutsEnabled;

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

        final DriverExecutionProfile profile = driverContext.getConfig().getProfile(profileName);

        this.maxRetryCount = MAX_RETRIES.getValue(profile, Integer.class);
        this.fixedBackOffTimeInMillis = FIXED_BACKOFF_TIME.getValue(profile, Integer.class);
        this.growingBackOffTimeInMillis = GROWING_BACKOFF_TIME.getValue(profile, Integer.class);
        this.retryReadTimeoutsEnabled = RETRY_READ_TIMEOUTS.getValue(profile, Boolean.class);
        this.retryWriteTimeoutsEnabled = RETRY_WRITE_TIMEOUTS.getValue(profile, Boolean.class);
    }

    CosmosRetryPolicy(final int maxRetryCount) {
        this(
            maxRetryCount,
            FIXED_BACKOFF_TIME.getDefaultValue(Integer.class),
            GROWING_BACKOFF_TIME.getDefaultValue(Integer.class),
            RETRY_READ_TIMEOUTS.getDefaultValue(Boolean.class),
            RETRY_WRITE_TIMEOUTS.getDefaultValue(Boolean.class));
    }

    CosmosRetryPolicy(
        final int maxRetryCount,
        final int fixedBackOffTimeInMillis,
        final int growingBackoffTimeInMillis,
        final boolean retryReadTimeoutsEnabled,
        final boolean retryWriteTimeoutsEnabled) {

        this.maxRetryCount = maxRetryCount;
        this.fixedBackOffTimeInMillis = fixedBackOffTimeInMillis;
        this.growingBackOffTimeInMillis = growingBackoffTimeInMillis;
        this.retryReadTimeoutsEnabled = retryReadTimeoutsEnabled;
        this.retryWriteTimeoutsEnabled = retryWriteTimeoutsEnabled;
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
     *
     * @return the {@code max-retries} value specified by this {@link CosmosRetryPolicy} object.
     */
    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    /**
     * Gets the {@code retry-read-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     *
     * @return The {@code retry-read-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     */
    public boolean isRetryReadTimeoutsEnabled() {
        return this.retryReadTimeoutsEnabled;
    }

    /**
     * Gets the {@code retry-write-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     *
     * @return The {@code retry-write-timeouts} value specified by this {@link CosmosRetryPolicy} object.
     */
    public boolean isRetryWriteTimeoutsEnabled() {
        return this.retryWriteTimeoutsEnabled;
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
            LOG.trace("onErrorResponse(request: {}, error: {}, retryCount: {}",
                toJson(request),
                toJson(error),
                toJson(retryCount));
        }

        RetryDecision retryDecision = RetryDecision.RETHROW;

        if (error instanceof OverloadedException || error instanceof WriteFailureException) {
            if (this.maxRetryCount == -1 || retryCount < this.maxRetryCount) {
                int retryAfterMillis = getRetryAfterMillis(error.toString());
                if (retryAfterMillis == -1) {
                    retryAfterMillis = this.maxRetryCount == -1
                        ? this.fixedBackOffTimeInMillis
                        : retryCount * this.growingBackOffTimeInMillis + RANDOM.nextInt(GROWING_BACKOFF_SALT_IN_MILLIS);
                }
                try {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("--> sleeping on {} for {} before retrying",
                            Thread.currentThread(),
                            Duration.ofMillis(retryAfterMillis));
                    }
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
     * Retries on read timeouts can be disabled by setting {@link CosmosRetryPolicyOption#RETRY_READ_TIMEOUTS} to
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
     * or equal to the minimum number of replica acknowledgements/response required to fulfill the operation (the value
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

        if (!this.retryReadTimeoutsEnabled) {
            return RetryDecision.RETHROW;
        }

        final RetryDecision decision = retryCount == 0 && received >= blockFor && !dataPresent
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_SAME && LOG.isDebugEnabled()) {
            LOG.debug(
                "retrying on read timeout on same host: { "
                    + "consistencyLevel: {}, "
                    + "blockFor: {}, "
                    + "dataPresent: {}, "
                    + "received: {}, "
                    + "retryCount: {} }",
                consistencyLevel,
                blockFor,
                false,
                received,
                retryCount);
        }

        return decision;
    }

    /**
     * Returns a {@link RetryDecision#RETRY_SAME retry-same} or {@link RetryDecision#RETHROW} decision when a request is
     * aborted before a response is received.
     * <p>
     * This can happen in two cases:
     * <p><ul>
     * <li>The connection was closed due to an external event. This will manifest as a {@link
     * ClosedConnectionException}
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

        return error instanceof ClosedConnectionException || error instanceof HeartbeatException
            ? this.retryManyTimesOrThrow(retryCount)
            : RetryDecision.RETHROW;
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

        return this.retryManyTimesOrThrow(retryCount);
    }

    /**
     * Returns a {@link RetryDecision#RETRY_SAME retry-same} or {@link RetryDecision#RETHROW rethrow} decision in
     * response to a {@link WriteTimeoutException write timeout error}.
     * <p>
     * This method is only invoked for idempotent statements. It is similar to {link #onReadTimeout}, but for write
     * operations.
     * <p>
     * Retries on write timeouts can be disabled by setting {@link CosmosRetryPolicyOption#RETRY_WRITE_TIMEOUTS} to
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

        if (!this.retryReadTimeoutsEnabled) {
            return RetryDecision.RETHROW;
        }

        final RetryDecision decision = retryCount == 0 && writeType == DefaultWriteType.BATCH_LOG
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_SAME && LOG.isDebugEnabled()) {
            LOG.debug(
                "retrying on write timeout on same host: { "
                    + "consistencyLevel: {}, "
                    + "writeType: {}, "
                    + "blockFor: {}, "
                    + "received: {}, "
                    + "retryCount: {} }",
                consistencyLevel,
                writeType,
                blockFor,
                received,
                retryCount);
        }

        return decision;
    }

    @Override
    public String toString() {
        return Json.toString(this);
    }

    // endregion

    // region Privates

    // Example errorMessage:
    // "com.datastax.driver.core.exceptions.OverloadedException: Queried host (babas.cassandra.cosmos.azure.com/40
    // .65.106.154:10350)
    // was overloaded: Request rate is large: ActivityID=98f98762-512e-442d-b5ef-36f5d03d788f, RetryAfterMs=10,
    // Additional details='
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
