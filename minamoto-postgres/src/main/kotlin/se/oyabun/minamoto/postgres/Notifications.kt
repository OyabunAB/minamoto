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
package se.oyabun.minamoto.postgres

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import se.oyabun.aelv.None
import se.oyabun.aelv.await
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.subscribeOn
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.Listener
import se.oyabun.minamoto.NotificationChannel
import se.oyabun.minamoto.NotificationSerializer
import se.oyabun.minamoto.Notifier
import se.oyabun.minamoto.PauseBehavior
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.AcquireResult
import se.oyabun.minamoto.pool.MinamotoPool
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean

private val log = Logging.of<PostgresListener<*>>()

internal class PostgresListener<T>(
    override val channel:    NotificationChannel<T>,
    override val serializer: NotificationSerializer<T>,
    private  val pool:       MinamotoPool,
    private  val behavior:   PauseBehavior,
    private  val handler:    (T) -> None<Unit>,
) : Listener<T> {

    private val stopped = AtomicBoolean(false)
    private val paused  = AtomicBoolean(false)
    private val _active = AtomicBoolean(false)
    override val isActive: Boolean get() = _active.get()

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    override fun pause()  { paused.set(true)  }
    override fun resume() { paused.set(false) }

    override fun stop() {
        stopped.set(true)
        _active.set(false)
    }

    override fun start() {
        stopped.set(false)
        val shutdownHook = Thread({ stop() }, "minamoto-listener-shutdown-${channel.name}")
        Runtime.getRuntime().addShutdownHook(shutdownHook)
        scope.launch {
            try {
                listenLoop()
            } finally {
                _active.set(false)
                runCatching { Runtime.getRuntime().removeShutdownHook(shutdownHook) }
            }
        }
    }

    private suspend fun listenLoop() {
        var backoffMs = 100L
        while (!stopped.get()) {
            val result = runCatching { acquireAndListen() }
            if (stopped.get()) break
            if (result.isFailure) {
                log.connection.error(
                    se.oyabun.minamoto.ConnectionId(-1),
                    result.exceptionOrNull() ?: RuntimeException("unknown error")
                )
                delay(backoffMs)
                backoffMs = (backoffMs * 2).coerceAtMost(30_000L)
            } else {
                backoffMs = 100L
            }
        }
    }

    private suspend fun acquireAndListen() {
        val acquired = pool.acquireSlot().await().rightOrThrow()
        if (acquired !is AcquireResult.Acquired) {
            throw DatabaseException.AcquireTimeout(pool.config.acquireTimeout)
        }
        val connection = acquired.slot.connection as PostgresConnection
        _active.set(true)

        val queue: Channel<BackendMessageNotification> = when (val b = behavior) {
            is PauseBehavior.Buffer -> if (b.maxSize == Int.MAX_VALUE) Channel(Channel.UNLIMITED) else Channel(b.maxSize)
            PauseBehavior.Discard   -> Channel(Channel.UNLIMITED)
        }

        // Compute the enqueue function once so the hot path in the Netty IO handler
        // does not branch on behavior on every notification.
        val enqueue: (BackendMessageNotification) -> Unit = when (val b = behavior) {
            is PauseBehavior.Buffer -> when {
                b.maxSize == Int.MAX_VALUE ->
                    { notification -> queue.trySend(notification) }
                b.overflow == PauseBehavior.Buffer.Overflow.DropOldest ->
                    { notification ->
                        if (queue.trySend(notification).isFailure) {
                            queue.tryReceive() // evict head to make room
                            queue.trySend(notification)
                        }
                    }
                else -> // DropNewest: trySend failure on a full channel drops the arriving item
                    { notification -> queue.trySend(notification) }
            }
            PauseBehavior.Discard ->
                { notification -> if (!paused.get()) queue.trySend(notification) }
        }

        connection.notificationHandler = { msg ->
            enqueue(BackendMessageNotification(msg.channel, msg.payload, msg.processId))
        }

        try {
            sendSimple(connection, "LISTEN ${channel.name}")
            while (!stopped.get()) {
                if (paused.get()) {
                    delay(50)
                    continue
                }
                val notification = queue.tryReceive()
                if (notification.isFailure) { delay(10); continue }
                if (notification.isClosed) break
                val msg = notification.getOrNull() ?: continue
                if (msg.channel != channel.name) continue
                val decoded = runCatching {
                    val bytes = Base64.getDecoder().decode(msg.payload)
                    serializer.decode(bytes)
                }.getOrElse { e ->
                    log.connection.error(connection.id, e)
                    continue
                }
                runCatching {
                    handler(decoded).await().rightOrThrow()
                }.onFailure { e ->
                    log.connection.error(connection.id, e)
                }
            }
        } finally {
            connection.notificationHandler = null
            _active.set(false)
            if (!connection.state.isUsable) {
                pool.invalidate(connection.id).await()
            } else {
                runCatching { sendSimple(connection, "UNLISTEN ${channel.name}") }
                pool.release(connection.id).await()
            }
            queue.close()
        }
    }

    private suspend fun sendSimple(connection: PostgresConnection, sql: String) {
        connection.executeSimpleCommand(sql).await().rightOrThrow()
    }
}

internal class PostgresNotifier<T>(
    override val channel:    NotificationChannel<T>,
    override val serializer: NotificationSerializer<T>,
    private  val pool:       MinamotoPool,
    private  val database:   PostgresDatabase,
) : Notifier<T> {

    override fun notify(value: T): None<Unit> {
        val encoded = Base64.getEncoder().encodeToString(serializer.encode(value))
        val escaped = encoded.replace("'", "''")
        return database.run("NOTIFY ${channel.name}, '$escaped'").execute()
            .subscribeOn(PoolContext(pool))
    }
}

private data class BackendMessageNotification(
    val channel:   String,
    val payload:   String,
    val processId: Int,
)
