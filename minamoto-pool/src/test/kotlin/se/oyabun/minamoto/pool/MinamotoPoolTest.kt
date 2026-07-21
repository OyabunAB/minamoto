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
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.await
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.DatabaseException
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
        override fun create(): One<Connection> = One.defer {
            onCreate()
            object : Connection {
                override val id    = ConnectionId(idCounter.incrementAndGet())
                override val state = ConnectionState.Idle
                override fun ping(): One<ValidationResult> =
                    if (healthy) One.single(ValidationResult.Valid)
                    else         One.single(ValidationResult.Invalid("unhealthy"))
                override fun close(): None<Unit> = None.defer { onDestroy() }
                override fun begin(definition: se.oyabun.minamoto.TransactionDefinition): None<Unit> = None.complete()
                override fun commit(): None<Unit> = None.complete()
                override fun rollback(): None<Unit> = None.complete()
                override fun savepoint(id: se.oyabun.minamoto.SavepointId): None<Unit> = None.complete()
                override fun releaseSavepoint(id: se.oyabun.minamoto.SavepointId): None<Unit> = None.complete()
                override fun rollbackToSavepoint(id: se.oyabun.minamoto.SavepointId): None<Unit> = None.complete()
            }
        }
        override fun validate(connection: Connection): One<ValidationResult> = connection.ping()
        override fun destroy(connection: Connection): None<Unit> = connection.close()
    }

    private fun failingFactory(failCount: Int): ConnectionFactory {
        var attempts = 0
        return object : ConnectionFactory {
            override fun create(): One<Connection> = One.defer {
                if (++attempts <= failCount) throw RuntimeException("transient failure")
                fakeFactory().create().await().rightOrThrow()
            }
            override fun validate(connection: Connection): One<ValidationResult> = One.single(ValidationResult.Valid)
            override fun destroy(connection: Connection): None<Unit> = None.complete()
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
            val result = pool.acquireSlot().await().rightOrThrow()
            assertIs<AcquireResult.Acquired>(result)
            pool.close().await()
        }
    }

    @Test
    fun `release returns slot to pool`() = runBlocking {
        withTimeout(5.seconds) {
            val pool  = pool(maxSize = 1, initialSize = 1)
            Thread.sleep(50)
            val first = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            pool.release(first.slot.id).await()

            val second = pool.acquireSlot().await().rightOrThrow()
            assertIs<AcquireResult.Acquired>(second)
            assertEquals(first.slot.id, (second as AcquireResult.Acquired).slot.id)
            pool.close().await()
        }
    }

    @Test
    fun `acquire times out when pool exhausted`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool(maxSize = 1, initialSize = 1, acquireTimeout = 100.milliseconds)
            Thread.sleep(50)
            pool.acquireSlot().await().rightOrThrow()

            val result = pool.acquireSlot().await().rightOrThrow()
            assertIs<AcquireResult.TimedOut>(result)
            pool.close().await()
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

            val result = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            pool.invalidate(result.slot.id).await()
            Thread.sleep(50)

            assertEquals(2, created)
            assertEquals(1, destroyed)
            pool.close().await()
        }
    }

    @Test
    fun `postAllocate hook fires on acquire`() = runBlocking {
        withTimeout(5.seconds) {
            var fired = false
            val pool  = pool(postAllocate = ConnectionHook.Action { fired = true })
            Thread.sleep(50)
            pool.acquireSlot().await().rightOrThrow()
            pool.close().await()

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

            assertFailsWith<DatabaseException.ConnectionLost> { pool.acquireSlot().await().rightOrThrow() }
            assertEquals(1, destroyed)
            pool.close().await()
        }
    }

    @Test
    fun `preRelease hook fires on release`() = runBlocking {
        withTimeout(5.seconds) {
            var fired = false
            val pool  = pool(preRelease = ConnectionHook.Action { fired = true })
            Thread.sleep(50)
            val result = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            pool.release(result.slot.id).await()
            pool.close().await()

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
            val result = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            pool.release(result.slot.id).await()
            Thread.sleep(50)

            assertEquals(1, destroyed)
            pool.close().await()
        }
    }

    @Test
    fun `release on unknown id is no-op`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool()
            Thread.sleep(50)
            pool.release(ConnectionId(999L)).await()
            pool.close().await()
        }
    }

    @Test
    fun `invalidate on unknown id is no-op`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool()
            Thread.sleep(50)
            pool.invalidate(ConnectionId(999L)).await()
            pool.close().await()
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
            val result = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            Thread.sleep(10)
            pool.release(result.slot.id).await()
            Thread.sleep(50)

            assertEquals(1, destroyed)
            pool.close().await()
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
            val result = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            Thread.sleep(10)
            pool.release(result.slot.id).await()
            Thread.sleep(50)

            assertEquals(1, destroyed)
            pool.close().await()
        }
    }

    @Test
    fun `validation rejects unhealthy connection and acquires replacement`() = runBlocking {
        withTimeout(5.seconds) {
            var callCount = 0
            val factory = object : ConnectionFactory {
                override fun create(): One<Connection> = One.defer {
                    val healthy = callCount++ > 0
                    object : Connection {
                        override val id    = ConnectionId(idCounter.incrementAndGet())
                        override val state = ConnectionState.Idle
                        override fun ping(): One<ValidationResult> =
                            if (healthy) One.single(ValidationResult.Valid)
                            else         One.single(ValidationResult.Invalid("bad"))
                        override fun close(): None<Unit> = None.complete()
                        override fun begin(definition: se.oyabun.minamoto.TransactionDefinition): None<Unit> = None.complete()
                        override fun commit(): None<Unit> = None.complete()
                        override fun rollback(): None<Unit> = None.complete()
                        override fun savepoint(id: se.oyabun.minamoto.SavepointId): None<Unit> = None.complete()
                        override fun releaseSavepoint(id: se.oyabun.minamoto.SavepointId): None<Unit> = None.complete()
                        override fun rollbackToSavepoint(id: se.oyabun.minamoto.SavepointId): None<Unit> = None.complete()
                    }
                }
                override fun validate(connection: Connection): One<ValidationResult> = connection.ping()
                override fun destroy(connection: Connection): None<Unit> = None.complete()
            }
            val pool = pool(factory = factory, validation = ValidationQuery.Local, maxSize = 2)
            Thread.sleep(50)

            val result = pool.acquireSlot().await().rightOrThrow()
            assertIs<AcquireResult.Acquired>(result)
            pool.close().await()
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

            val result = pool.acquireSlot().await().rightOrThrow()
            assertIs<AcquireResult.Acquired>(result)
            pool.close().await()
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
            pool.close().await()

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

            val slot = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            assertEquals(1, pool.statistics.idle)
            assertEquals(1, pool.statistics.acquired)

            pool.release(slot.slot.id).await()
            pool.close().await()
        }
    }

    @Test
    fun `minIdle replacement is acquirable after invalidation`() = runBlocking {
        withTimeout(5.seconds) {
            val pool = pool(maxSize = 1, initialSize = 1, minIdle = 1)
            Thread.sleep(50)

            val first = pool.acquireSlot().await().rightOrThrow() as AcquireResult.Acquired
            pool.invalidate(first.slot.id).await()
            Thread.sleep(100)

            val second = pool.acquireSlot().await().rightOrThrow()
            assertIs<AcquireResult.Acquired>(second)
            pool.close().await()
        }
    }
}
