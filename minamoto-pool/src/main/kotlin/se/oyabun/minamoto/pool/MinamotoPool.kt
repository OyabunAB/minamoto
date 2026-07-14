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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
import se.oyabun.minamoto.ConnectionContext
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.ValidationResult
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
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
 */
class MinamotoPool(
    override val config:  PoolConfig,
    private  val factory: ConnectionFactory,
) : ConnectionPool {

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

    override suspend fun acquire(): AcquireResult {
        val connectionContext = coroutineContext[ConnectionContext]

        // Deadlock detection — all slots already held by this coroutine chain
        if (connectionContext != null && connectionContext.held.size >= config.maxSize) {
            return AcquireResult.DeadlockPrevented(connectionContext.held.size, config.maxSize)
        }

        maybeCreateSlot()

        waiting.incrementAndGet()
        return try {
            val slot = idle.asOne()
                .await(config.acquireTimeout)
                .getOrThrow()
                .copy(state = SlotState.Acquired, lastUsed = System.nanoTime())
                .also { slots[it.id] = it }

            // Validate with timeout if configured
            if (config.validation !is ValidationQuery.None) {
                val result = One.defer { factory.validate(slot.connection) }
                    .await(config.validationTimeout)
                    .fold(
                        onLeft  = { throw MinamotoException.ValidationFailed("timeout", it) },
                        onRight = { it },
                    )
                if (result is ValidationResult.Invalid) {
                    invalidate(slot.id)
                    return acquire()
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

            connectionContext?.acquire(slot.id)
            AcquireResult.Acquired(slot)
        } catch (e: se.oyabun.aelv.TimeoutException) {
            AcquireResult.TimedOut
        } finally {
            waiting.decrementAndGet()
        }
    }

    override suspend fun release(id: ConnectionId) {
        val slot = slots[id] ?: return

        // preRelease — failure invalidates rather than returning to pool
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

        val now        = System.nanoTime()
        val age        = now - slot.createdAt
        val idleTime   = now - slot.lastUsed

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
        None.defer<Unit> { maybeCreateSlot() }.await()
    }

    override suspend fun close() {
        scope.cancel()
        idle.complete()
        Many.from(slots.values.toList())
            .doOnNext(action = suspend { slot: PoolSlot ->
                try { factory.destroy(slot.connection) } catch (_: Exception) {}
            })
            .discard().await()
        slots.clear()
        total.set(0)
    }

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
}
