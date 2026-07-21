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
package se.oyabun.minamoto

import se.oyabun.aelv.None

/**
 * A typed PostgreSQL NOTIFY/LISTEN channel.
 *
 * Defines the channel name used in `LISTEN`/`NOTIFY` statements.
 * The payload type [T] is carried at the type level — the wire format
 * is always a string, encoded and decoded by a [NotificationSerializer].
 */
interface NotificationChannel<T> {
    val name: String
}

/**
 * Serializes and deserializes notification payloads to/from [ByteArray].
 *
 * Using [ByteArray] as the canonical form enables the same serializer
 * to be shared across transports (PG NOTIFY, Kafka, HTTP, etc.).
 * The payload is Base64-encoded for transmission over the PG wire protocol.
 */
interface NotificationSerializer<T> {
    fun encode(value: T): ByteArray
    fun decode(bytes: ByteArray): T
}

/**
 * Controls what happens to notifications that arrive while a [Listener] is paused.
 *
 * PostgreSQL NOTIFY is fire-and-forget at the protocol level — the server delivers
 * to every connected listener and discards immediately. There is no server-side
 * per-subscriber queue and no flow-control mechanism. Any buffering must therefore
 * happen client-side, which is what [Buffer] provides.
 */
sealed interface PauseBehavior {

    /**
     * Accumulate notifications in a client-side queue while paused; deliver them in
     * arrival order when [Listener.resume] is called.
     *
     * [maxSize] bounds the queue to prevent heap growth under high notification rates.
     * When the queue is full, [overflow] determines which item is dropped.
     * Defaults to an unbounded queue.
     */
    data class Buffer(
        val maxSize:  Int      = Int.MAX_VALUE,
        val overflow: Overflow = Overflow.DropNewest,
    ) : PauseBehavior {
        enum class Overflow {
            /** Drop the arriving notification when the buffer is full. */
            DropNewest,
            /** Evict the oldest buffered notification to make room for the arriving one. */
            DropOldest,
        }
    }

    /**
     * Drop notifications that arrive while paused.
     *
     * Notifications already in transit on the wire when [Listener.pause] is called
     * may still be delivered — NOTIFY has no acknowledgement or recall mechanism.
     */
    data object Discard : PauseBehavior
}

/**
 * A long-lived, self-healing PostgreSQL LISTEN subscription.
 *
 * Acquires a dedicated connection on [start], subscribes to [channel],
 * and drives [handler] for each incoming notification. On connection
 * failure the listener re-acquires a connection and re-subscribes
 * automatically. [stop] sends `UNLISTEN`, releases the connection, and
 * completes the [start] pipeline.
 */
interface Listener<T> {
    val channel:    NotificationChannel<T>
    val serializer: NotificationSerializer<T>

    fun start()
    fun pause()
    fun resume()
    fun stop()

    val isActive: Boolean
}

/**
 * Sends a single PostgreSQL `NOTIFY` on demand.
 *
 * Borrows a connection from the pool for each [notify] call — no
 * dedicated connection is held between calls.
 */
interface Notifier<T> {
    val channel:    NotificationChannel<T>
    val serializer: NotificationSerializer<T>

    fun notify(value: T): None<Unit>
}
