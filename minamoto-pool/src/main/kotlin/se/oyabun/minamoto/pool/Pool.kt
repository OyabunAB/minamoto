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
package se.oyabun.minamoto.pool

import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@JvmInline
value class PoolName(val value: String)

/**
 * A lifecycle hook invoked at a specific point in the connection borrow cycle.
 *
 * [NoOp] is the default — use it when no action is needed.
 * [Action] wraps a suspend function to execute at the hook point.
 */
sealed interface ConnectionHook {
    data object NoOp                               : ConnectionHook
    data class  Action(val block: suspend () -> Unit) : ConnectionHook
}

/**
 * Configuration for a [ConnectionPool].
 *
 * [initialSize] connections are created eagerly at pool startup. The pool will never
 * exceed [maxSize] total connections, and will keep at least [minIdle] connections
 * alive when idle.
 *
 * [postAllocate] runs after a connection is taken from the pool, before it reaches the caller.
 * [preRelease] runs before a connection is returned to the pool — use it to roll back open
 * transactions or reset session state.
 *
 * Timeouts:
 * - [acquireTimeout] — how long a caller may wait for a free slot
 * - [createTimeout] — how long a new physical connection may take to establish
 * - [validationTimeout] — how long a validation query may run before the connection is considered broken
 *
 * Eviction:
 * - [idleTimeout] — connections idle longer than this are eligible for eviction
 * - [maxLifetime] — connections older than this are evicted regardless of idle state
 * - [acquireRetry] — how many times to retry creating a new connection on transient failure
 */
data class PoolConfig(
    val name:              PoolName         = PoolName("minamoto-pool"),
    val initialSize:       Int              = 2,
    val minIdle:           Int              = 1,
    val maxSize:           Int              = 10,
    val acquireTimeout:    Duration         = 30.seconds,
    val createTimeout:     Duration         = 30.seconds,
    val validationTimeout: Duration         = 5.seconds,
    val idleTimeout:       Duration         = 10.minutes,
    val maxLifetime:       Duration         = 30.minutes,
    val acquireRetry:      Int              = 1,
    val validation:        ValidationQuery  = ValidationQuery.Local,
    val eviction:          EvictionPolicy   = EvictionPolicy.OnRelease,
    val postAllocate:      ConnectionHook   = ConnectionHook.NoOp,
    val preRelease:        ConnectionHook   = ConnectionHook.NoOp,
)

/**
 * How the pool checks that a connection is alive before handing it to a caller.
 *
 * [Local] inspects the connection state without a network round-trip — fast but cannot
 * detect a silently dropped TCP connection.
 *
 * [Remote] sends a query to the server — reliable but adds latency on every acquire.
 * Defaults to `SELECT 1`; override with a lighter or driver-specific ping if available.
 *
 * [None] skips validation entirely. Use only when latency is critical and the network
 * is known to be stable, or when [PoolConfig.preRelease] already ensures connection health.
 */
sealed interface ValidationQuery {
    data object Local                                  : ValidationQuery
    data class  Remote(val query: String = "SELECT 1") : ValidationQuery
    data object None                                   : ValidationQuery
}

/**
 * When the pool sweeps for and evicts stale connections.
 *
 * [OnRelease] checks on every release — no background goroutine, low overhead,
 * but eviction lags until the next release occurs.
 *
 * [Scheduled] additionally runs a background sweep at [interval]. Useful when the
 * pool is mostly idle and connections would otherwise sit stale until the next use.
 *
 * [Never] disables eviction. Connections live until [ConnectionPool.invalidate] or
 * [ConnectionPool.close] is called explicitly.
 */
sealed interface EvictionPolicy {
    data object OnRelease                                      : EvictionPolicy
    data class  Scheduled(val interval: Duration = 1.minutes) : EvictionPolicy
    data object Never                                          : EvictionPolicy
}

sealed interface SlotState {
    data object Idle       : SlotState
    data object Acquired   : SlotState
    data object Validating : SlotState
    data object Evicting   : SlotState
    data object Closed     : SlotState
}

/**
 * A single managed slot in the pool, wrapping a physical [Connection].
 *
 * [createdAt] and [lastUsed] are [System.nanoTime] values used for [idleTimeout]
 * and [maxLifetime] eviction decisions.
 */
data class PoolSlot(
    val connection: Connection,
    val state:      SlotState,
    val createdAt:  Long,
    val lastUsed:   Long,
) {
    val id: ConnectionId get() = connection.id
}

/**
 * A point-in-time snapshot of pool activity.
 *
 * [waiting] is the number of callers currently suspended waiting for a free slot.
 * A consistently non-zero [PoolStatistics.waiting] value indicates the pool [PoolConfig.maxSize] is too small.
 */
data class PoolStatistics(
    val name:     PoolName,
    val total:    Int,
    val idle:     Int,
    val acquired: Int,
    val waiting:  Int,
)

/**
 * The result of a [ConnectionPool.acquire] call.
 *
 * Always handle all three cases:
 * - [Acquired] — a slot is ready, proceed with the connection
 * - [TimedOut] — no slot became available within [PoolConfig.acquireTimeout]
 * - [DeadlockPrevented] — all pool connections are already held by this coroutine chain;
 *   proceeding would deadlock. Restructure the code to avoid nested independent acquisitions,
 *   or increase [PoolConfig.maxSize].
 */
sealed interface AcquireResult {
    data class  Acquired(val slot: PoolSlot)                        : AcquireResult
    data object TimedOut                                            : AcquireResult
    data class  DeadlockPrevented(val held: Int, val poolSize: Int) : AcquireResult
}

/**
 * A managed pool of physical database connections.
 *
 * Acquire a connection with [acquire], use it, then return it with [release].
 * If the connection is known to be broken, call [invalidate] instead — the pool
 * will discard it and create a replacement if [PoolConfig.minIdle] requires one.
 *
 * The pool consults [se.oyabun.minamoto.ConnectionContext] on every [acquire] to
 * detect whether the current coroutine chain already holds all available connections,
 * preventing self-deadlocks in recursive [se.oyabun.aelv.Many.flatMap] pipelines.
 */
interface ConnectionPool {
    val config:     PoolConfig
    val statistics: PoolStatistics

    suspend fun acquire(): AcquireResult
    suspend fun release(id: ConnectionId)
    suspend fun invalidate(id: ConnectionId)
    suspend fun close()
}
