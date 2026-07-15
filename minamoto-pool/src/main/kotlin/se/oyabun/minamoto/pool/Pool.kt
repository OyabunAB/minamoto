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

/** Identifies a pool instance in logs and diagnostics. */
@JvmInline
value class PoolName(val value: String)

/**
 * Configuration for a [ConnectionPool].
 *
 * [initialSize] connections are created eagerly at pool startup. The pool will never
 * exceed [maxSize] total connections, and will keep at least [minIdle] connections
 * alive when idle.
 *
 * [postAllocate] runs after a connection is taken from the pool, before it reaches the caller.
 * A failure in [postAllocate] invalidates the slot — it is not returned to the pool.
 *
 * [preRelease] runs before a connection is returned to the pool — use it to roll back open
 * transactions or reset session state. A failure in [preRelease] also invalidates the slot.
 *
 * Timeouts:
 * - [acquireTimeout] — how long a caller may wait for a free slot
 * - [createTimeout] — how long a new physical connection may take to establish
 * - [validationTimeout] — how long a validation query may run before the connection is discarded
 *
 * Eviction:
 * - [idleTimeout] — connections idle longer than this are evicted on release or background sweep
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
 * A lifecycle hook invoked at a specific point in the connection borrow cycle.
 *
 * [NoOp] is the default. [Action] wraps a suspend function called at the hook point.
 * Errors thrown by [Action] are treated as fatal — the connection is invalidated.
 */
sealed interface ConnectionHook {
    data object NoOp                                  : ConnectionHook
    data class  Action(val block: suspend () -> Unit) : ConnectionHook
}

/**
 * How the pool checks that a connection is alive before handing it to a caller.
 *
 * [Local] inspects the connection state without a network round-trip — fast but cannot
 * detect a silently dropped TCP connection.
 *
 * [Remote] sends a query to the server — reliable but adds latency on every acquire.
 * Defaults to `SELECT 1`; override with a driver-specific ping if available.
 *
 * [None] skips validation entirely. Safe when the network is stable and [preRelease]
 * already ensures connection health before returning to the pool.
 */
sealed interface ValidationQuery {
    data object Local                                    : ValidationQuery
    data class  Remote(val query: String = "SELECT 1")   : ValidationQuery
    data object None                                     : ValidationQuery
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

/**
 * The lifecycle state of a single slot in the pool.
 *
 * Transitions: `Idle` → `Acquired` (on [ConnectionPool.acquire]) →
 * `Idle` (on [ConnectionPool.release]) or destroyed (on [ConnectionPool.invalidate]).
 * `Validating` and `Evicting` are transient states during acquire and background sweep respectively.
 */
sealed interface SlotState {
    data object Idle       : SlotState
    data object Acquired   : SlotState
    data object Validating : SlotState
    data object Evicting   : SlotState
    data object Closed     : SlotState
}

/**
 * A single managed slot in the pool.
 *
 * Callers receive a [PoolSlot] inside [AcquireResult.Acquired]. The slot is immutable —
 * the pool copies it with updated state on each transition. [id] delegates to the
 * underlying [Connection.id].
 *
 * [createdAt] and [lastUsed] are [System.nanoTime] values used for [PoolConfig.idleTimeout]
 * and [PoolConfig.maxLifetime] eviction decisions.
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
 * [total] is all slots currently alive regardless of state.
 * [idle] is slots available for immediate acquisition.
 * [acquired] is slots currently held by callers.
 * [waiting] is callers currently suspended waiting for a free slot — a consistently
 * non-zero value indicates [PoolConfig.maxSize] is too small for the workload.
 */
data class PoolStatistics(
    val name:     PoolName,
    val total:    Int,
    val idle:     Int,
    val acquired: Int,
    val waiting:  Int,
)

/**
 * The result of a [ManagedPool.acquire] call.
 *
 * Always handle all three cases:
 * - [Acquired] — a slot is ready
 * - [TimedOut] — no slot became available within [PoolConfig.acquireTimeout]
 * - [DeadlockPrevented] — all pool connections are already held by this coroutine chain.
 *   [held] is the number of connections held; [poolSize] is [PoolConfig.maxSize].
 *   Restructure the code to avoid nested independent acquisitions, or increase [PoolConfig.maxSize].
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
 * discards it and creates a replacement if [PoolConfig.minIdle] requires one.
 *
 * [release] on an unknown id is a no-op. [invalidate] on an unknown id is a no-op.
 * Double-release of the same id returns the slot twice — callers must not release twice.
 *
 * The pool consults [se.oyabun.minamoto.ConnectionContext] on every [acquire] to detect
 * whether the current coroutine chain already holds all available connections, preventing
 * self-deadlocks in recursive [se.oyabun.aelv.Many.flatMap] pipelines.
 */
interface ManagedPool : se.oyabun.minamoto.ConnectionPool {

    val config:     PoolConfig
    val statistics: PoolStatistics

    /**
     * Acquire a connection slot.
     *
     * Suspends until a slot is available or [PoolConfig.acquireTimeout] elapses.
     * Validates the connection before returning if [PoolConfig.validation] is not [ValidationQuery.None].
     * Runs [PoolConfig.postAllocate] before returning — a failure invalidates the slot and throws.
     */
    suspend fun acquireSlot(): AcquireResult

    /**
     * Return a slot to the pool.
     *
     * Runs [PoolConfig.preRelease] first — a failure invalidates rather than returning the slot.
     * Slots past [PoolConfig.maxLifetime] or [PoolConfig.idleTimeout] are evicted rather than returned.
     */
    /** Returns the [Connection] for [id] if it is currently acquired by this pool, or null if not found. */
    fun connectionFor(id: se.oyabun.minamoto.ConnectionId): se.oyabun.minamoto.Connection?

    override suspend fun release(id: ConnectionId)

    /**
     * Discard a slot permanently.
     *
     * The underlying connection is destroyed. A replacement is created asynchronously
     * if [PoolConfig.minIdle] requires one.
     */
    suspend fun invalidate(id: ConnectionId)

    /** Close all connections and shut down the pool. In-progress acquisitions receive [AcquireResult.TimedOut]. */
    suspend fun close()
}
