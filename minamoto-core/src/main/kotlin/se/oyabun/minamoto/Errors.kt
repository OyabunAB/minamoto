/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.minamoto

/**
 * Sealed base for all exceptions originating from minamoto.
 *
 * Catch this single type to handle any library failure,
 * then narrow with `when` for specific handling.
 */
sealed class MinamotoException(message: String, cause: Throwable? = null) : RuntimeException(message, cause) {

    /** The requested column does not exist in the result row. */
    class UnknownColumn(name: String) :
        MinamotoException("unknown column: $name")

    /** A value retrieved from a row was null but a non-null type was requested. */
    class UnexpectedNull(column: String) :
        MinamotoException("unexpected null value for column: $column")

    /** The connection pool could not acquire a connection within the allowed time. */
    class PoolTimeout(message: String) :
        MinamotoException(message)

    /** The connection pool could not acquire a connection within [se.oyabun.minamoto.pool.PoolConfig.acquireTimeout]. */
    class AcquireTimeout(timeout: kotlin.time.Duration) :
        MinamotoException("pool acquire timed out after $timeout")

    /** A pooled connection failed validation before being returned to a caller. */
    class ValidationFailed(reason: String, cause: Throwable? = null) :
        MinamotoException("connection validation failed: $reason", cause)

    /**
     * Acquiring a connection would deadlock the current coroutine chain —
     * all connections from this pool are already held by this chain.
     */
    class DeadlockDetected(held: Int, poolSize: Int) :
        MinamotoException("deadlock detected: coroutine chain holds $held/$poolSize connections from the same pool")

    /** The connection was closed unexpectedly. */
    class ConnectionClosed(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /** Authentication with the database server failed. */
    class AuthenticationFailed(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /**
     * The database server rejected the query.
     *
     * [sqlState] is the 5-character SQLSTATE code from the server — use it to identify
     * specific error conditions programmatically (e.g. `23505` for unique constraint violation).
     * [severity] is the server-reported severity level (ERROR, FATAL, PANIC).
     * [detail] and [hint] carry the optional extended fields from the server error response.
     */
    class QueryFailed(
        message:          String,
        val sqlState:     String,
        val severity:     String,
        val detail:       String = "",
        val hint:         String = "",
        cause:            Throwable? = null,
    ) : MinamotoException(message, cause)

    /** A transaction could not be committed. */
    class CommitFailed(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /** A transaction rollback failed. */
    class RollbackFailed(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /** The connection to the server was lost. */
    class ConnectionLost(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /** TLS negotiation or certificate validation failed. */
    class TlsFailed(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /** A codec could not encode or decode a value. */
    class CodecFailed(message: String, cause: Throwable? = null) :
        MinamotoException(message, cause)

    /** The operation was attempted on a closed or invalid resource. */
    class InvalidState(message: String) :
        MinamotoException(message)
}
