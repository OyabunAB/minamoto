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
@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
package se.oyabun.minamoto.pool

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import se.oyabun.aelv.Maybe
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.await
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.discard
import se.oyabun.aelv.doOnNext
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.getOrThrow
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.drain
import se.oyabun.aelv.retry
import se.oyabun.aelv.TimeoutException
import se.oyabun.aelv.resource
import kotlin.coroutines.CoroutineContext
import kotlin.internal.LowPriorityInOverloadResolution
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionContext
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionStack
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.PoolId
import se.oyabun.minamoto.SavepointId
import se.oyabun.minamoto.TransactionBoundary
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.TransactionId
import se.oyabun.minamoto.ValidationResult
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

/**
 * A connection pool backed by [UnicastSink] — each acquired slot goes to exactly one caller.
 *
 * Slots are pre-created up to [PoolConfig.initialSize] at startup and topped up to
 * [PoolConfig.minIdle] after each release. The pool never exceeds [PoolConfig.maxSize]
 * total connections.
 *
 * Backpressure is natural: callers acquiring when all slots are busy suspend on
 * [idle.asOne().await(acquireTimeout)]. No polling, no spin-waiting.
 *
 * [connectionBudget] is a shared [Semaphore] owned by the database — it enforces the
 * total live connection ceiling across all pools that target the same database server.
 * A permit is acquired before creating a physical connection and released on destroy.
 */
class MinamotoPool(
    override val config:            PoolConfig,
    private  val factory:           ConnectionFactory,
    private  val connectionBudget:  Semaphore? = null,
) : ManagedPool {

    override val poolId: PoolId = PoolId(poolIdCounter.incrementAndGet())

    private val idle       = Sinks.unicast<PoolSlot>()
    private val slots      = ConcurrentHashMap<ConnectionId, PoolSlot>()
    private val total      = AtomicInteger(0)
    private val waiting    = AtomicInteger(0)
    private val scope      = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val createLock = Mutex()

    override val statistics: PoolStatistics
        get() = PoolStatistics(
            name     = config.name,
            total    = total.get(),
            idle     = slots.values.count { it.state is SlotState.Idle },
            acquired = slots.values.count { it.state is SlotState.Acquired },
            waiting  = waiting.get(),
        )

    init {
        scope.launch { warmUp() }
        if (config.eviction is EvictionPolicy.Scheduled) {
            Many.interval(config.eviction.interval).drain(
                onNext     = { evict() },
                onError    = { /* eviction errors are non-fatal */ },
                onComplete = { },
            )
        }
    }

    override suspend fun acquireSlot(): AcquireResult {
        val connectionContext = coroutineContext[ConnectionContext]

        // Deadlock detection — this coroutine chain already holds connections from this pool
        // and the pool is at capacity. Blocking would guarantee a deadlock.
        if (connectionContext != null) {
            val heldFromThisPool = connectionContext.heldCountFor(poolId)
            if (heldFromThisPool > 0 && total.get() >= config.maxSize &&
                slots.values.none { it.state is SlotState.Idle }) {
                return AcquireResult.DeadlockPrevented(heldFromThisPool, config.maxSize)
            }
        }

        maybeCreateSlot()

        waiting.incrementAndGet()
        return try {
            val slot = idle.asOne()
                .await(config.acquireTimeout)
                .getOrThrow()
                .copy(state = SlotState.Acquired, lastUsed = System.nanoTime())
                .also { slots[it.id] = it }

            if (config.validation !is ValidationQuery.None) {
                val result = One.defer { factory.validate(slot.connection) }
                    .await(config.validationTimeout)
                    .fold(
                        onLeft  = { throw MinamotoException.ValidationFailed("timeout", it) },
                        onRight = { it },
                    )
                if (result is ValidationResult.Invalid) {
                    invalidate(slot.id)
                    return acquireSlot()
                }
            }

            try {
                when (val hook = config.postAllocate) {
                    is ConnectionHook.NoOp   -> { }
                    is ConnectionHook.Action -> hook.block()
                }
            } catch (e: Exception) {
                invalidate(slot.id)
                throw MinamotoException.ConnectionLost("postAllocate failed", e)
            }

            connectionContext?.acquire(slot.id, poolId)
            AcquireResult.Acquired(slot)
        } catch (e: se.oyabun.aelv.TimeoutException) {
            AcquireResult.TimedOut
        } finally {
            waiting.decrementAndGet()
        }
    }

    override suspend fun release(id: ConnectionId) {
        val slot = slots[id] ?: return

        try {
            when (val hook = config.preRelease) {
                is ConnectionHook.NoOp   -> { /* nothing */ }
                is ConnectionHook.Action -> hook.block()
            }
        } catch (e: Exception) {
            invalidate(id)
            return
        }

        coroutineContext[ConnectionContext]?.release(id)

        val now      = System.nanoTime()
        val age      = now - slot.createdAt
        val idleTime = now - slot.lastUsed

        if (age > config.maxLifetime.inWholeNanoseconds ||
            idleTime > config.idleTimeout.inWholeNanoseconds) {
            invalidate(id)
            return
        }

        val released = slot.copy(state = SlotState.Idle, lastUsed = now)
        slots[id] = released
        idle.emit(released)

        if (config.eviction is EvictionPolicy.OnRelease) evict()

        maybeCreateSlot()
    }

    override suspend fun invalidate(id: ConnectionId) {
        val slot = slots.remove(id) ?: return
        total.decrementAndGet()
        runCatching { factory.destroy(slot.connection) }
        connectionBudget?.release()
        None.defer<Unit> { maybeCreateSlot() }.await()
    }

    override suspend fun close() {
        scope.cancel()
        idle.complete()
        Many.from(slots.values.toList())
            .doOnNext(action = suspend { slot: PoolSlot ->
                try {
                    factory.destroy(slot.connection)
                    connectionBudget?.release()
                } catch (_: Exception) {}
            })
            .discard().await()
        slots.clear()
        total.set(0)
    }

    /**
     * Runs [block] with this pool installed in the coroutine context.
     *
     * Every pipeline subscribed inside [block] that resolves a connection will acquire one
     * from this pool. The innermost enclosing pool wins when scopes are nested.
     * No transaction is started — each pipeline runs in autocommit mode unless wrapped
     * by [transactionally].
     */
    suspend operator fun <T> invoke(block: suspend () -> T): T =
        withContext(currentCoroutineContext() + PoolContext(this)) {
            block()
        }

    /**
     * Runs [block] with this pool installed in the coroutine context and a transaction active.
     *
     * Acquires a connection, sends `BEGIN` with [definition], installs both in the coroutine
     * context, then runs [block]. On normal exit sends `COMMIT`; on any exception sends
     * `ROLLBACK` before rethrowing.
     *
     * Nested [transactionally] calls detect the active transaction via [ConnectionContext]:
     * - Same pool, already in a transaction → creates a savepoint instead of a new `BEGIN`
     * - Different pool → independent transaction on that pool's connection
     */
    @Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
    suspend fun <T> transactionally(
        definition: TransactionDefinition = TransactionDefinition(),
        block:      suspend () -> T,
    ): T {
        val handle = openTransaction(definition)
        return try {
            val result = withContext(handle.txContext) { block() }
            closeTransaction(handle, null)
            result
        } catch (thrown: Throwable) {
            closeTransaction(handle, thrown)
            throw thrown
        }
    }

    /**
     * Wraps a [Many] pipeline in a transaction.
     *
     * The connection is acquired and `BEGIN` is sent when the pipeline is subscribed.
     * `COMMIT` fires on normal completion, `ROLLBACK` on error. The connection is held
     * for the full lifetime of the stream — rows are streamed inside the open transaction.
     *
     * The returned [Many] is a pure pipeline component — fully chainable with no suspension
     * at definition time. Nested calls on the same pool use a savepoint automatically.
     */
    fun <T : Any> transaction(
        definition: TransactionDefinition = TransactionDefinition(),
        block:      () -> Many<T>,
    ): Many<T> = Many.resource(
        acquire = { openTransaction(definition) },
        release = { handle, error -> closeTransaction(handle, error) },
        use     = { handle -> Many.defer(factory = suspend { withContext(handle.txContext) { block() } }) },
    )

    /**
     * Acquires a connection, sends `BEGIN`, and builds the composite [CoroutineContext] to install.
     *
     * For nested calls on the same pool, issues `SAVEPOINT` instead. The returned [TransactionHandle]
     * carries everything [closeTransaction] needs to commit or roll back correctly.
     */
    private suspend fun openTransaction(definition: TransactionDefinition): TransactionHandle {
        val outerContext      = currentCoroutineContext()
        val connectionContext = outerContext[ConnectionContext] ?: ConnectionContext()
        val existingId        = connectionContext.activeConnectionId()
        val existingPoolId    = connectionContext.activePoolId()

        if (existingId != null && existingPoolId == poolId) {
            val savepointId = SavepointId("sp_${savepointCounter.incrementAndGet()}")
            val slot        = slots[existingId]
                ?: throw MinamotoException.InvalidState("active connection not found in pool slots")
            slot.connection.savepoint(savepointId)
            val newFrame = ConnectionStack.Frame(
                connection  = existingId,
                poolId      = poolId,
                transaction = TransactionBoundary.Savepoint(savepointId),
                parent      = connectionContext.stack,
            )
            return TransactionHandle(
                connection  = slot.connection,
                slotId      = existingId,
                savepoint   = savepointId,
                txContext   = outerContext + connectionContext.push(newFrame) + PoolContext(this),
                releaseSlot = false,
            )
        }

        val acquireResult = acquireSlot()
        val slot = when (acquireResult) {
            is AcquireResult.Acquired          -> acquireResult.slot
            is AcquireResult.TimedOut          -> throw MinamotoException.AcquireTimeout(config.acquireTimeout)
            is AcquireResult.DeadlockPrevented -> throw MinamotoException.DeadlockDetected(
                acquireResult.held, acquireResult.poolSize
            )
        }
        val newFrame = ConnectionStack.Frame(
            connection  = slot.id,
            poolId      = poolId,
            transaction = TransactionBoundary.Root(TransactionId(transactionIdCounter.incrementAndGet()), definition),
            parent      = connectionContext.stack,
        )
        slot.connection.begin(definition)
        return TransactionHandle(
            connection  = slot.connection,
            slotId      = slot.id,
            savepoint   = null,
            txContext   = outerContext + connectionContext.push(newFrame) + PoolContext(this),
            releaseSlot = true,
        )
    }

    private suspend fun closeTransaction(handle: TransactionHandle, error: Throwable?) {
        val savepoint = handle.savepoint
        if (savepoint != null) {
            if (error != null) runCatching { handle.connection.rollbackToSavepoint(savepoint) }
            else               runCatching { handle.connection.releaseSavepoint(savepoint) }
        } else {
            if (error != null) runCatching { handle.connection.rollback() }
            else               handle.connection.commit()
            if (handle.releaseSlot) release(handle.slotId)
        }
    }

    private data class TransactionHandle(
        val connection:  Connection,
        val slotId:      ConnectionId,
        val savepoint:   SavepointId?,
        val txContext:   CoroutineContext,
        val releaseSlot: Boolean,
    )

    private suspend fun warmUp() {
        Many.range(0, config.initialSize)
            .concatMap { _: Int ->
                if (total.get() < config.maxSize)
                    Many.defer(factory = suspend { createSlot(); Many.empty<Int>() })
                else
                    Many.empty()
            }
            .discard().await()
    }

    private suspend fun maybeCreateSlot() {
        val needsReplenishment = total.get() < config.minIdle
        val hasWaiters         = waiting.get() > 0 && total.get() < config.maxSize
        if (needsReplenishment || hasWaiters) createSlot()
    }

    private suspend fun createSlot() {
        createLock.withLock {
            if (total.get() >= config.maxSize) return
            // Acquire a permit from the shared database-wide connection budget before
            // opening a physical connection. Released in invalidate() and close().
            connectionBudget?.acquire()
            total.incrementAndGet()
            try {
                val connection = One.defer { factory.create() }
                    .retry(config.acquireRetry.toLong())
                    .await(config.createTimeout)
                    .getOrThrow()
                val slot = PoolSlot(
                    connection = connection,
                    state      = SlotState.Idle,
                    createdAt  = System.nanoTime(),
                    lastUsed   = System.nanoTime(),
                )
                slots[slot.id] = slot
                idle.emit(slot)
            } catch (e: Exception) {
                total.decrementAndGet()
                connectionBudget?.release()
                throw MinamotoException.ConnectionLost(
                    "failed to create connection after ${config.acquireRetry} retries", e
                )
            }
        }
    }

    private fun evict() {
        val now   = System.nanoTime()
        val stale = slots.values.filter { it.state is SlotState.Idle }.filter {
            now - it.createdAt > config.maxLifetime.inWholeNanoseconds ||
            now - it.lastUsed  > config.idleTimeout.inWholeNanoseconds
        }
        Many.from(stale)
            .flatMap { slot -> Many.defer(factory = suspend { invalidate(slot.id); Many.empty<Unit>() }) }
            .drain(onNext = {}, onError = {})
    }

    companion object {
        private val poolIdCounter        = AtomicLong(0)
        private val transactionIdCounter = AtomicLong(0)
        private val savepointCounter     = AtomicLong(0)
    }

    override fun connectionFor(id: ConnectionId): Connection? = slots[id]?.connection
    override suspend fun acquire(): se.oyabun.minamoto.ConnectionAcquireResult {
        return when (val result = acquireSlot()) {
            is AcquireResult.Acquired          -> se.oyabun.minamoto.ConnectionAcquireResult.Acquired(result.slot.connection)
            is AcquireResult.TimedOut          -> se.oyabun.minamoto.ConnectionAcquireResult.TimedOut
            is AcquireResult.DeadlockPrevented -> se.oyabun.minamoto.ConnectionAcquireResult.DeadlockDetected(result.held, result.poolSize)
        }
    }
}
