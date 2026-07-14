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

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.ValidationResult
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertIs
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MinamotoPoolTest {

    private val idCounter = AtomicLong(0)

    private fun fakeFactory(
        healthy: Boolean = true,
        onCreate: () -> Unit = {},
        onDestroy: () -> Unit = {},
    ): ConnectionFactory = object : ConnectionFactory {
        override suspend fun create(): Connection {
            onCreate()
            return object : Connection {
                override val id    = ConnectionId(idCounter.incrementAndGet())
                override val state = ConnectionState.Idle
                override suspend fun ping()  = if (healthy) ValidationResult.Valid
                                               else ValidationResult.Invalid("unhealthy")
                override suspend fun close() = onDestroy()
            }
        }
        override suspend fun validate(connection: Connection) = connection.ping()
        override suspend fun destroy(connection: Connection)  = connection.close()
    }

    private fun pool(
        factory:    ConnectionFactory = fakeFactory(),
        maxSize:    Int               = 2,
        initialSize: Int              = 1,
        minIdle:    Int               = 1,
        acquireTimeout: kotlin.time.Duration = 1.seconds,
        validation: ValidationQuery   = ValidationQuery.None,
    ) = MinamotoPool(
        config  = PoolConfig(
            maxSize        = maxSize,
            initialSize    = initialSize,
            minIdle        = minIdle,
            acquireTimeout = acquireTimeout,
            validation     = validation,
        ),
        factory = factory,
    )

    @Test
    fun `acquire returns slot when idle`() = runBlocking {
        withTimeout(5.seconds) {
            val pool   = pool()
            Thread.sleep(50)
            val result = pool.acquire()
            assertIs<AcquireResult.Acquired>(result)
            pool.close()
        }
    }

    @Test
    fun `release returns slot to pool`() = runBlocking {
        withTimeout(5.seconds) {
            val pool   = pool(maxSize = 1, initialSize = 1)
            Thread.sleep(50)
            val first  = pool.acquire() as AcquireResult.Acquired
            pool.release(first.slot.id)

            val second = pool.acquire()
            assertIs<AcquireResult.Acquired>(second)
            assertEquals(first.slot.id, second.slot.id)
            pool.close()
        }
    }

    @Test
    fun `acquire times out when pool exhausted`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool(maxSize = 1, initialSize = 1, acquireTimeout = 100.milliseconds)
            Thread.sleep(50)
            pool.acquire()

            val result = pool.acquire()
            assertIs<AcquireResult.TimedOut>(result)
            pool.close()
        }
    }

    @Test
    fun `invalidate destroys slot and creates replacement`() = runBlocking {
        withTimeout(5.seconds) {
            var created   = 0
            var destroyed = 0
            val factory   = fakeFactory(onCreate = { created++ }, onDestroy = { destroyed++ })
            val pool      = pool(factory = factory, maxSize = 1, initialSize = 1)
            Thread.sleep(50)

            val result = pool.acquire() as AcquireResult.Acquired
            pool.invalidate(result.slot.id)
            Thread.sleep(50)

            assertEquals(2, created)
            assertEquals(1, destroyed)
            pool.close()
        }
    }

    @Test
    fun `postAllocate hook fires on acquire`() = runBlocking {
        withTimeout(5.seconds) {
            var hookFired = false
            val pool = pool(
                factory = fakeFactory(),
                maxSize = 1,
                initialSize = 1,
            ).also { it.close() }

            val poolWithHook = MinamotoPool(
                config  = PoolConfig(
                    maxSize      = 1,
                    initialSize  = 1,
                    postAllocate = ConnectionHook.Action { hookFired = true },
                ),
                factory = fakeFactory(),
            )
            Thread.sleep(50)
            poolWithHook.acquire()
            poolWithHook.close()

            assertEquals(true, hookFired)
        }
    }

    @Test
    fun `preRelease hook fires on release`() = runBlocking {
        withTimeout(5.seconds) {
            var hookFired = false
            val poolWithHook = MinamotoPool(
                config  = PoolConfig(
                    maxSize     = 1,
                    initialSize = 1,
                    preRelease  = ConnectionHook.Action { hookFired = true },
                ),
                factory = fakeFactory(),
            )
            Thread.sleep(50)
            val result = poolWithHook.acquire() as AcquireResult.Acquired
            poolWithHook.release(result.slot.id)
            poolWithHook.close()

            assertEquals(true, hookFired)
        }
    }

    @Test
    fun `statistics reflect pool state`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool(maxSize = 2, initialSize = 2)
            Thread.sleep(50)

            assertEquals(2, pool.statistics.idle)
            assertEquals(0, pool.statistics.acquired)

            val slot = pool.acquire() as AcquireResult.Acquired
            assertEquals(1, pool.statistics.idle)
            assertEquals(1, pool.statistics.acquired)

            pool.release(slot.slot.id)
            pool.close()
        }
    }
}
