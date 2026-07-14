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
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.await
import se.oyabun.aelv.getOrThrow
import se.oyabun.minamoto.Connection
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
            scope.launch { runEvictionLoop(config.eviction.interval) }
        }
    }

    override suspend fun acquire(): AcquireResult {
        // Deadlock detection — check ConnectionContext on current coroutine
        val ctx = coroutineContext[ConnectionContext]
        if (ctx != null && ctx.held.size >= config.maxSize) {
            return AcquireResult.DeadlockPrevented(ctx.held.size, config.maxSize)
        }

        // Ensure at least one slot exists
        maybeCreateSlot()

        waiting.incrementAndGet()
        return try {
            val slot = idle.asOne()
                .await(config.acquireTimeout)
                .getOrThrow()
                .copy(state = SlotState.Acquired, lastUsed = System.nanoTime())
                .also { slots[it.id] = it }

            // Validate if configured
            if (config.validation !is ValidationQuery.None) {
                when (factory.validate(slot.connection)) {
                    is ValidationResult.Valid   -> { /* continue */ }
                    is ValidationResult.Invalid -> {
                        invalidate(slot.id)
                        return acquire() // retry with a fresh slot
                    }
                }
            }

            // Run postAllocate hook
            when (val hook = config.postAllocate) {
                is ConnectionHook.NoOp   -> { /* nothing */ }
                is ConnectionHook.Action -> hook.block()
            }

            ctx?.acquire(slot.id)
            AcquireResult.Acquired(slot)
        } catch (e: se.oyabun.aelv.TimeoutException) {
            AcquireResult.TimedOut
        } finally {
            waiting.decrementAndGet()
        }
    }

    override suspend fun release(id: ConnectionId) {
        val slot = slots[id] ?: return

        // Run preRelease hook
        when (val hook = config.preRelease) {
            is ConnectionHook.NoOp   -> { /* nothing */ }
            is ConnectionHook.Action -> hook.block()
        }

        coroutineContext[ConnectionContext]?.release(id)

        val now  = System.nanoTime()
        val age  = now - slot.createdAt
        val idle = now - slot.lastUsed

        // Evict if past lifetime or idle timeout
        if (age > config.maxLifetime.inWholeNanoseconds ||
            idle > config.idleTimeout.inWholeNanoseconds) {
            invalidate(id)
            return
        }

        val released = slot.copy(state = SlotState.Idle, lastUsed = now)
        slots[id] = released
        this.idle.emit(released)

        if (config.eviction is EvictionPolicy.OnRelease) evict()

        maybeCreateSlot()
    }

    override suspend fun invalidate(id: ConnectionId) {
        val slot = slots.remove(id) ?: return
        total.decrementAndGet()
        factory.destroy(slot.connection)
        maybeCreateSlot() // replace if minIdle requires it
    }

    override suspend fun close() {
        scope.cancel()
        idle.complete()
        slots.values.forEach { factory.destroy(it.connection) }
        slots.clear()
        total.set(0)
    }

    // ---------------------------------------------------------------------------

    private suspend fun warmUp() {
        repeat(config.initialSize) {
            if (total.get() < config.maxSize) createSlot()
        }
    }

    private suspend fun maybeCreateSlot() {
        if (total.get() < config.minIdle || waiting.get() > 0 && total.get() < config.maxSize) {
            createSlot()
        }
    }

    private suspend fun createSlot() {
        createLock.withLock {
            if (total.get() >= config.maxSize) return
            total.incrementAndGet()
            var attempt = 0
            while (attempt <= config.acquireRetry) {
                try {
                    val connection = factory.create()
                    val slot = PoolSlot(
                        connection = connection,
                        state      = SlotState.Idle,
                        createdAt  = System.nanoTime(),
                        lastUsed   = System.nanoTime(),
                    )
                    slots[slot.id] = slot
                    idle.emit(slot)
                    return
                } catch (e: Exception) {
                    attempt++
                    if (attempt > config.acquireRetry) {
                        total.decrementAndGet()
                        throw MinamotoException.ConnectionLost("failed to create connection after ${config.acquireRetry} retries", e)
                    }
                }
            }
        }
    }

    private fun evict() {
        val now = System.nanoTime()
        slots.values
            .filter { it.state is SlotState.Idle }
            .filter { now - it.createdAt > config.maxLifetime.inWholeNanoseconds ||
                      now - it.lastUsed  > config.idleTimeout.inWholeNanoseconds }
            .forEach { slot ->
                scope.launch { invalidate(slot.id) }
            }
    }

    private suspend fun runEvictionLoop(interval: kotlin.time.Duration) {
        while (true) {
            delay(interval)
            evict()
        }
    }
}
