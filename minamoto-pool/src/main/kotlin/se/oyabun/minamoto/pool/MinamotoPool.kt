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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import se.oyabun.aelv.Signal
import se.oyabun.aelv.await
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.discard
import se.oyabun.aelv.doFinally
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.or
import se.oyabun.aelv.toMany
import se.oyabun.aelv.flatMapNone
import se.oyabun.aelv.getOrThrow
import se.oyabun.aelv.map
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.drain
import se.oyabun.aelv.recover
import se.oyabun.aelv.resource
import se.oyabun.aelv.retry
import se.oyabun.aelv.subscribeOn
import se.oyabun.aelv.Either
import se.oyabun.aelv.then
import se.oyabun.aelv.thenReturn
import se.oyabun.aelv.toMany
import se.oyabun.aelv.TimeoutException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.internal.LowPriorityInOverloadResolution
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionContext
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionStack
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.PoolId
import se.oyabun.minamoto.SavepointId
import se.oyabun.minamoto.TransactionBoundary
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.TransactionId
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.ConnectionAcquireResult
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class MinamotoPool(
    override val config:           PoolConfig,
    private  val factory:          ConnectionFactory,
    private  val connectionBudget: Semaphore? = null,
) : ManagedPool {

    override val poolId: PoolId = PoolId(poolIdCounter.incrementAndGet())

    private val idleQueue  = ConcurrentLinkedQueue<PoolSlot>()
    private val idleSignal = Channel<Unit>(Channel.CONFLATED)
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
        scope.launch { warmUp().await() }
        if (config.eviction is EvictionPolicy.Scheduled)
            Many.interval(config.eviction.interval).drain(onNext = { evict() }, onError = {})
    }

    /**
     * Acquires one budget permit, creates a physical connection, then registers it as an idle slot.
     *
     * The budget permit and [total] counter are released on any failure so the pool
     * stays consistent without imperative try/finally.
     */
    private fun createSlot(): None<Connection> =
        None.defer<Unit> { connectionBudget?.acquire() }
            .then { factory.create().retry(config.acquireRetry.toLong()) }
            .flatMapNone { connection ->
                None.defer<Unit> {
                    createLock.withLock {
                        if (total.get() >= config.maxSize) {
                            factory.destroy(connection).await()
                            connectionBudget?.release()
                            return@withLock
                        }
                        total.incrementAndGet()
                        val slot = PoolSlot(connection, SlotState.Idle, System.nanoTime(), System.nanoTime())
                        slots[slot.id] = slot
                        idleQueue.add(slot)
                        idleSignal.trySend(Unit)
                    }
                }
            }
            .recover { e ->
                None.defer<Unit> { connectionBudget?.release() }
                    .then { None.error(DatabaseException.ConnectionLost(
                        "failed to create connection after ${config.acquireRetry} retries", e
                    )) }
            }

    private fun maybeCreateSlot(): None<Unit> {
        val needsReplenishment = total.get() < config.minIdle
        val hasCapacity        = total.get() < config.maxSize
        return if (needsReplenishment || (waiting.get() > 0 && hasCapacity))
            None.defer<Unit> { scope.launch { createSlot().await() } }
        else
            None.complete()
    }

    override fun acquireSlot(): One<AcquireResult> =
        One.defer { coroutineContext[ConnectionContext] ?: ConnectionContext() }
            .flatMap { ctx ->
                val held       = ctx.heldCountFor(poolId)
                val existingId = ctx.activeConnectionId()

                if (existingId != null && ctx.activePoolId() == poolId) {
                    val slot = slots[existingId] ?: return@flatMap One.error(DatabaseException.InvalidState("active connection not found in pool slots"))
                    ctx.acquire(existingId, poolId)
                    return@flatMap One.single(AcquireResult.Acquired(slot))
                }

                if (held > 0 && total.get() >= config.maxSize && slots.values.none { it.state is SlotState.Idle })
                    return@flatMap One.single(AcquireResult.DeadlockPrevented(held, config.maxSize))

                None.defer<Unit> { waiting.incrementAndGet() }
                    .then { maybeCreateSlot() }
                    .then {
                        One.generate<PoolSlot> { downstream ->
                            val deadline = System.nanoTime() + config.acquireTimeout.inWholeNanoseconds
                            while (true) {
                                val slot = idleQueue.poll()
                                if (slot != null) { downstream(Signal.Upstream.Next(slot)); break }
                                val remaining = (deadline - System.nanoTime()).coerceAtLeast(0)
                                if (remaining == 0L) { downstream(Signal.Upstream.Error(TimeoutException(config.acquireTimeout))); break }
                                withTimeoutOrNull(remaining / 1_000_000) { idleSignal.receive() }
                            }
                            downstream(Signal.Upstream.Complete)
                        }
                    }
                    .doFinally { waiting.decrementAndGet() }
                    .map { slot: PoolSlot -> slot.copy(state = SlotState.Acquired, lastUsed = System.nanoTime()).also { slots[it.id] = it } }
                    .flatMap { slot -> validateAndHook(slot) }
                    .map { slot -> ctx.acquire(slot.id, poolId); AcquireResult.Acquired(slot) as AcquireResult }
                    .recover { e -> if (e is TimeoutException) AcquireResult.TimedOut else throw e }
            }

    private fun validateAndHook(slot: PoolSlot): One<PoolSlot> =
        (if (config.validation is ValidationQuery.None) One.single(slot)
        else factory.validate(slot.connection)
            .map { validity: ValidationResult ->
                if (validity is ValidationResult.Invalid) throw DatabaseException.ValidationFailed("invalid: ${validity.reason}", null)
                slot
            }
        ).flatMap { s: PoolSlot ->
            when (val hook = config.postAllocate) {
                is ConnectionHook.NoOp   -> One.single(s)
                is ConnectionHook.Action -> None.defer<Unit> { hook.block() }
                    .recover { e: Exception -> None.error(DatabaseException.ConnectionLost("postAllocate failed", e)) }
                    .thenReturn(s)
            }
        }.recover { e: Exception ->
            invalidate(slot.id)
            throw e
        }

    override fun release(id: ConnectionId): None<Unit> {
        val slot = slots[id] ?: return None.complete()
        val now  = System.nanoTime()
        return when (val hook = config.preRelease) {
            is ConnectionHook.NoOp   -> None.complete()
            is ConnectionHook.Action -> None.defer<Unit> { hook.block() }
                .recover { e -> invalidate(id); return@recover null as Nothing }
        }
        .then {
            None.defer<Unit> { coroutineContext[ConnectionContext]?.release(id) }
        }
        .then {
            if (now - slot.createdAt > config.maxLifetime.inWholeNanoseconds ||
                now - slot.lastUsed  > config.idleTimeout.inWholeNanoseconds)
                invalidate(id)
            else None.defer<Unit> {
                slots[id] = slot.copy(state = SlotState.Idle, lastUsed = now)
                idleQueue.add(slots[id]!!)
                idleSignal.trySend(Unit)
                if (config.eviction is EvictionPolicy.OnRelease) evict()
            }.then { maybeCreateSlot() }
        }
    }

    override fun invalidate(id: ConnectionId): None<Unit> {
        val slot = slots.remove(id) ?: return None.complete()
        total.decrementAndGet()
        return factory.destroy(slot.connection)
            .recover { None.complete<Unit>() }
            .then { None.defer<Unit> { connectionBudget?.release(); maybeCreateSlot().await() } }
    }

    override fun close(): None<Unit> =
        Many.from(slots.values.toList())
            .flatMapNone { slot ->
                factory.destroy(slot.connection)
                    .recover { None.complete<Unit>() }
                    .then { None.defer<Unit> { connectionBudget?.release() } }
            }
            .then {
                None.defer<Unit> {
                    scope.cancel()
                    idleSignal.close()
                    slots.clear()
                    total.set(0)
                }
            }

    private fun warmUp(): None<Int> =
        Many.range(0, config.initialSize)
            .flatMapNone { _: Int ->
                if (total.get() < config.maxSize) createSlot()
                else None.complete<Connection>()
            }

    private fun evict() {
        val now   = System.nanoTime()
        val stale = slots.values.filter { it.state is SlotState.Idle }.filter {
            now - it.createdAt > config.maxLifetime.inWholeNanoseconds ||
            now - it.lastUsed  > config.idleTimeout.inWholeNanoseconds
        }
        scope.launch {
            Many.from(stale)
                .flatMapNone { slot -> invalidate(slot.id) }
                .await()
        }
    }

    suspend operator fun <T> invoke(block: suspend () -> T): T =
        withContext(currentCoroutineContext() + PoolContext(this)) { block() }

    /**
     * Wraps a pipeline with this pool's context, installed at subscription time.
     *
     * Each pipeline element that resolves a connection will acquire one from this pool.
     * No transaction is started — use [transaction] for transactional pipelines.
     */
    fun <T : Any> scoped(pipeline: Many<T>): Many<T> = pipeline.subscribeOn(PoolContext(this))
    fun <T : Any> scoped(pipeline: One<T>):  One<T>  = pipeline.subscribeOn(PoolContext(this))

    @Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
    suspend fun <T> transactionally(
        definition: TransactionDefinition = TransactionDefinition(),
        block:      suspend () -> T,
    ): T {
        val handle = openTransaction(definition).await().getOrThrow()
        return try {
            val result = withContext(handle.txContext) { block() }
            closeTransaction<Unit>(handle, Either.success(Unit)).await()
            result
        } catch (thrown: Throwable) {
            closeTransaction<Unit>(handle, Either.failure(thrown)).await()
            throw thrown
        }
    }

    override fun <T : Any> transactionally(
        definition: TransactionDefinition,
        block:      () -> One<T>,
    ): One<T> = Many.resource(
        acquire = { openTransaction(definition) },
        release = { handle, signal -> closeTransaction<T>(handle, signal) },
        use     = { handle -> block().subscribeOn(handle.txContext).toMany() },
    ).firstMaybe().or { throw DatabaseException.InvalidState("transactionally block returned no value") }

    override fun <T : Any> transactionally(
        definition: TransactionDefinition,
        block:      () -> Many<T>,
    ): Many<T> = Many.resource(
        acquire = { openTransaction(definition) },
        release = { handle, signal -> closeTransaction<T>(handle, signal) },
        use     = { handle -> block().subscribeOn(handle.txContext) },
    )

    private fun openTransaction(definition: TransactionDefinition): One<TransactionHandle> =
        acquireSlot()
            .flatMap(transform = suspend { result: AcquireResult ->
                val slot = when (result) {
                    is AcquireResult.Acquired          -> result.slot
                    is AcquireResult.TimedOut          -> return@flatMap One.error(DatabaseException.AcquireTimeout(config.acquireTimeout))
                    is AcquireResult.DeadlockPrevented -> return@flatMap One.error(DatabaseException.DeadlockDetected(result.held, result.poolSize))
                }
                val outerContext      = currentCoroutineContext()
                val connectionContext = outerContext[ConnectionContext] ?: ConnectionContext()
                val existingId        = connectionContext.activeConnectionId()

                if (existingId != null && connectionContext.activePoolId() == poolId) {
                    val savepointId  = SavepointId("sp_${savepointCounter.incrementAndGet()}")
                    val existingSlot = slots[existingId]
                        ?: return@flatMap One.error(DatabaseException.InvalidState("active connection not found in pool slots"))
                    val handle = TransactionHandle(
                        connection  = existingSlot.connection,
                        slotId      = existingId,
                        savepoint   = savepointId,
                        txContext   = outerContext + connectionContext.push(ConnectionStack.Frame(existingId, poolId, TransactionBoundary.Savepoint(savepointId), connectionContext.stack)) + PoolContext(this@MinamotoPool),
                        releaseSlot = false,
                    )
                    existingSlot.connection.savepoint(savepointId)
                        .then { One.single(handle) }
                } else {
                    val transactionId = TransactionId(transactionIdCounter.incrementAndGet())
                    val handle = TransactionHandle(
                        connection  = slot.connection,
                        slotId      = slot.id,
                        savepoint   = null,
                        txContext   = outerContext + connectionContext.push(ConnectionStack.Frame(slot.id, poolId, TransactionBoundary.Root(transactionId, definition), connectionContext.stack)) + PoolContext(this@MinamotoPool),
                        releaseSlot = true,
                    )
                    slot.connection.begin(definition)
                        .then { One.single(handle) }
                }
            })

    private fun <T : Any> closeTransaction(handle: TransactionHandle, signal: Either<Throwable, Unit>): None<T> {
        val savepoint = handle.savepoint
        @Suppress("UNCHECKED_CAST")
        return if (savepoint != null) {
            if (signal is Either.Left)
                handle.connection.rollbackToSavepoint(savepoint).recover { None.complete() }
            else
                handle.connection.releaseSavepoint(savepoint).recover { None.complete() }
        } else {
            val endTransaction = if (signal is Either.Left)
                handle.connection.rollback().recover { None.complete() }
            else
                handle.connection.commit()
            if (handle.releaseSlot)
                endTransaction.then { release(handle.slotId) }
            else
                endTransaction
        } as None<T>
    }

    private data class TransactionHandle(
        val connection:  Connection,
        val slotId:      ConnectionId,
        val savepoint:   SavepointId?,
        val txContext:   CoroutineContext,
        val releaseSlot: Boolean,
    )

    companion object {
        private val poolIdCounter        = AtomicLong(0)
        private val transactionIdCounter = AtomicLong(0)
        private val savepointCounter     = AtomicLong(0)
    }

    override fun connectionFor(id: ConnectionId): Connection? = slots[id]?.connection
    override fun acquiredConnections(): List<Connection> = slots.values.filter { it.state is SlotState.Acquired }.map { it.connection }

    override fun acquire(): One<ConnectionAcquireResult> =
        acquireSlot().map { result ->
            when (result) {
                is AcquireResult.Acquired          -> ConnectionAcquireResult.Acquired(result.slot.connection)
                is AcquireResult.TimedOut          -> ConnectionAcquireResult.TimedOut
                is AcquireResult.DeadlockPrevented -> ConnectionAcquireResult.DeadlockDetected(result.held, result.poolSize)
            }
        }
}

