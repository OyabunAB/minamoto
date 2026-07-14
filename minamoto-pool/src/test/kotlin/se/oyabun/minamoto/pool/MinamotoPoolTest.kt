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
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.ValidationResult
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertIs
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MinamotoPoolTest {

    private val idCounter = AtomicLong(0)

    private fun fakeFactory(
        healthy:   Boolean   = true,
        onCreate:  () -> Unit = {},
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

    private fun failingFactory(failCount: Int): ConnectionFactory {
        var attempts = 0
        return object : ConnectionFactory {
            override suspend fun create(): Connection {
                if (++attempts <= failCount) throw RuntimeException("transient failure")
                return fakeFactory().create()
            }
            override suspend fun validate(connection: Connection) = ValidationResult.Valid
            override suspend fun destroy(connection: Connection)  {}
        }
    }

    private fun pool(
        factory:        ConnectionFactory            = fakeFactory(),
        maxSize:        Int                          = 2,
        initialSize:    Int                          = 1,
        minIdle:        Int                          = 1,
        acquireTimeout: kotlin.time.Duration         = 1.seconds,
        validation:     ValidationQuery              = ValidationQuery.None,
        eviction:       EvictionPolicy               = EvictionPolicy.OnRelease,
        postAllocate:   ConnectionHook               = ConnectionHook.NoOp,
        preRelease:     ConnectionHook               = ConnectionHook.NoOp,
        idleTimeout:    kotlin.time.Duration         = 10.seconds,
        maxLifetime:    kotlin.time.Duration         = 30.seconds,
    ) = MinamotoPool(
        config = PoolConfig(
            maxSize        = maxSize,
            initialSize    = initialSize,
            minIdle        = minIdle,
            acquireTimeout = acquireTimeout,
            validation     = validation,
            eviction       = eviction,
            postAllocate   = postAllocate,
            preRelease     = preRelease,
            idleTimeout    = idleTimeout,
            maxLifetime    = maxLifetime,
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
            val pool  = pool(maxSize = 1, initialSize = 1)
            Thread.sleep(50)
            val first = pool.acquire() as AcquireResult.Acquired
            pool.release(first.slot.id)

            val second = pool.acquire()
            assertIs<AcquireResult.Acquired>(second)
            assertEquals(first.slot.id, (second as AcquireResult.Acquired).slot.id)
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
            val pool      = pool(
                factory     = fakeFactory(onCreate = { created++ }, onDestroy = { destroyed++ }),
                maxSize     = 1,
                initialSize = 1,
            )
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
            var fired = false
            val pool  = pool(postAllocate = ConnectionHook.Action { fired = true })
            Thread.sleep(50)
            pool.acquire()
            pool.close()

            assertTrue(fired)
        }
    }

    @Test
    fun `postAllocate failure invalidates slot and throws`() = runBlocking {
        withTimeout(5.seconds) {
            var destroyed = 0
            val pool = pool(
                factory      = fakeFactory(onDestroy = { destroyed++ }),
                maxSize      = 1,
                initialSize  = 1,
                postAllocate = ConnectionHook.Action { throw RuntimeException("hook failure") },
            )
            Thread.sleep(50)

            assertFailsWith<MinamotoException.ConnectionLost> { pool.acquire() }
            assertEquals(1, destroyed)
            pool.close()
        }
    }

    @Test
    fun `preRelease hook fires on release`() = runBlocking {
        withTimeout(5.seconds) {
            var fired = false
            val pool  = pool(preRelease = ConnectionHook.Action { fired = true })
            Thread.sleep(50)
            val result = pool.acquire() as AcquireResult.Acquired
            pool.release(result.slot.id)
            pool.close()

            assertTrue(fired)
        }
    }

    @Test
    fun `preRelease failure invalidates slot instead of returning to pool`() = runBlocking {
        withTimeout(5.seconds) {
            var destroyed = 0
            val pool = pool(
                factory    = fakeFactory(onDestroy = { destroyed++ }),
                maxSize    = 1,
                initialSize = 1,
                preRelease = ConnectionHook.Action { throw RuntimeException("hook failure") },
            )
            Thread.sleep(50)
            val result = pool.acquire() as AcquireResult.Acquired
            pool.release(result.slot.id)
            Thread.sleep(50)

            assertEquals(1, destroyed)
            pool.close()
        }
    }

    @Test
    fun `release on unknown id is no-op`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool()
            Thread.sleep(50)
            pool.release(ConnectionId(999L))
            pool.close()
        }
    }

    @Test
    fun `invalidate on unknown id is no-op`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool()
            Thread.sleep(50)
            pool.invalidate(ConnectionId(999L))
            pool.close()
        }
    }

    @Test
    fun `slot evicted when maxLifetime exceeded on release`() = runBlocking {
        withTimeout(5.seconds) {
            var destroyed = 0
            val pool = pool(
                factory     = fakeFactory(onDestroy = { destroyed++ }),
                maxSize     = 1,
                initialSize = 1,
                maxLifetime = 1.milliseconds,
            )
            Thread.sleep(50)
            val result = pool.acquire() as AcquireResult.Acquired
            Thread.sleep(10)
            pool.release(result.slot.id)
            Thread.sleep(50)

            assertEquals(1, destroyed)
            pool.close()
        }
    }

    @Test
    fun `slot evicted when idleTimeout exceeded on release`() = runBlocking {
        withTimeout(5.seconds) {
            var destroyed = 0
            val pool = pool(
                factory     = fakeFactory(onDestroy = { destroyed++ }),
                maxSize     = 1,
                initialSize = 1,
                idleTimeout = 1.milliseconds,
            )
            Thread.sleep(50)
            val result = pool.acquire() as AcquireResult.Acquired
            Thread.sleep(10)
            pool.release(result.slot.id)
            Thread.sleep(50)

            assertEquals(1, destroyed)
            pool.close()
        }
    }

    @Test
    fun `validation rejects unhealthy connection and acquires replacement`() = runBlocking {
        withTimeout(5.seconds) {
            var callCount = 0
            val factory = object : ConnectionFactory {
                override suspend fun create(): Connection {
                    val healthy = callCount++ > 0
                    return object : Connection {
                        override val id    = ConnectionId(idCounter.incrementAndGet())
                        override val state = ConnectionState.Idle
                        override suspend fun ping()  = if (healthy) ValidationResult.Valid
                                                       else ValidationResult.Invalid("bad")
                        override suspend fun close() {}
                    }
                }
                override suspend fun validate(connection: Connection) = connection.ping()
                override suspend fun destroy(connection: Connection)  {}
            }
            val pool = pool(factory = factory, validation = ValidationQuery.Local, maxSize = 2)
            Thread.sleep(50)

            val result = pool.acquire()
            assertIs<AcquireResult.Acquired>(result)
            pool.close()
        }
    }

    @Test
    fun `acquireRetry succeeds after transient factory failure`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = MinamotoPool(
                config  = PoolConfig(maxSize = 1, initialSize = 1, acquireRetry = 2),
                factory = failingFactory(failCount = 1),
            )
            Thread.sleep(100)

            val result = pool.acquire()
            assertIs<AcquireResult.Acquired>(result)
            pool.close()
        }
    }

    @Test
    fun `close destroys all slots`() = runBlocking {
        withTimeout(5.seconds) {
            var destroyed = 0
            val pool = pool(
                factory     = fakeFactory(onDestroy = { destroyed++ }),
                maxSize     = 2,
                initialSize = 2,
            )
            Thread.sleep(50)
            pool.close()

            assertEquals(2, destroyed)
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

    @Test
    fun `minIdle replacement is acquirable after invalidation`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool(maxSize = 1, initialSize = 1, minIdle = 1)
            Thread.sleep(50)

            val first = pool.acquire() as AcquireResult.Acquired
            pool.invalidate(first.slot.id)
            Thread.sleep(100)

            val second = pool.acquire()
            assertIs<AcquireResult.Acquired>(second)
            pool.close()
        }
    }
}
