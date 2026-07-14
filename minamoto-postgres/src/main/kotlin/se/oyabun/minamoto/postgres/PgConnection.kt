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

import io.netty.buffer.ByteBufAllocator
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import se.oyabun.aelv.Many
import se.oyabun.aelv.ReplaySink
import se.oyabun.aelv.await
import se.oyabun.aelv.discard
import se.oyabun.aelv.drain
import se.oyabun.aelv.netty.NettyConnection
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.netty.inbound
import se.oyabun.aelv.netty.write
import se.oyabun.aelv.publishOn
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.postgres.protocol.BackendMessage
import se.oyabun.minamoto.postgres.protocol.BackendMessage.*
import se.oyabun.minamoto.postgres.protocol.FrontendMessage
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.*
import se.oyabun.minamoto.postgres.protocol.MessageDecoder
import se.oyabun.minamoto.postgres.protocol.MessageEncoder
import se.oyabun.minamoto.postgres.protocol.framed
import se.oyabun.minamoto.postgres.protocol.handshake
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * A physical PostgreSQL connection over a Netty channel.
 *
 * All operations are modeled as [Conversation]s — strictly FIFO, one active at a time.
 * The [InboundHandler] is installed in the pipeline during channel initialization so
 * no lifecycle events are missed before the first subscription.
 */
internal class PgConnection(
    override val id:         ConnectionId,
    private  val connection: NettyConnection,
    private  val transport:  NettyTransport,
    private  val allocator:  ByteBufAllocator = connection.channel.alloc(),
) : Connection {

    private val log = Logging.of<PgConnection>()

    override var state: ConnectionState = ConnectionState.Idle
        private set

    private val writeMutex    = Mutex()
    private val conversations = ConcurrentLinkedQueue<Conversation>()

    private val subscription = connection.inbound()
        .framed(allocator)
        .publishOn(MinamotoDispatchers.connection)
        .drain(
            onNext = { buf ->
                val message = try {
                    MessageDecoder.decode(buf)
                } finally {
                    buf.release()
                }
                log.protocol.messageReceived(id, message::class.simpleName ?: "Unknown")
                when (message) {
                    is NoticeResponse       -> return@drain
                    is NotificationResponse -> return@drain
                    is ParameterStatus      -> return@drain
                    else                    -> { /* route to active conversation */ }
                }
                val conversation = conversations.peek()
                if (conversation == null) {
                    log.protocol.noConversation(id, message.toString())
                    return@drain
                }

                if (!conversation.sink.tryEmit(message)) {
                    log.connection.invalidState(id, "conversation sink full — backpressure violation")
                    return@drain
                }

                if (conversation.takeUntil(message)) {
                    conversations.poll()
                    log.protocol.conversationComplete(id)
                    conversation.sink.complete()
                }
            },
            onError = { e ->
                log.connection.error(id, e)
                state = ConnectionState.Closed
                conversations.forEach { it.sink.error(e) }
                conversations.clear()
            },
            onComplete = {
                log.connection.closed(id)
                state = ConnectionState.Closed
                conversations.forEach { it.sink.complete() }
                conversations.clear()
            },
        )

    /**
     * Send [messages] to the server and stream responses until [takeUntil] returns true.
     *
     * Writes and conversation registration are atomic under [writeMutex] — the conversation
     * is in the queue before its messages hit the wire, preserving FIFO ordering.
     */
    suspend fun exchange(
        messages:  List<FrontendMessage>,
        takeUntil: (BackendMessage) -> Boolean,
    ): Many<BackendMessage> {
        val sink         = ReplaySink<BackendMessage>()
        val conversation = Conversation(takeUntil, sink)

        writeMutex.withLock {
            conversations.add(conversation)
            log.protocol.conversationQueued(id, conversations.size)
            messages.forEach { msg ->
                log.protocol.messageSent(id, msg::class.simpleName ?: "Unknown")
                connection.write(MessageEncoder.encode(msg, allocator)).await()
            }
        }

        return sink.asMany()
    }

    override suspend fun ping(): ValidationResult =
        try {
            exchange(
                messages = listOf(
                    Parse("", "SELECT 1"),
                    Bind("", "", emptyList()),
                    Execute("", 1),
                    Sync,
                ),
                takeUntil = { it is ReadyForQuery },
            ).discard().await()
            ValidationResult.Valid
        } catch (e: Exception) {
            ValidationResult.Invalid(e.message ?: "ping failed")
        }

    override suspend fun close() {
        log.connection.closing(id)
        state = ConnectionState.Closing
        subscription.cancel()
        connection.write(MessageEncoder.encode(Terminate, allocator)).await()
        connection.channel.close().sync()
        log.connection.closed(id)
        state = ConnectionState.Closed
    }
}

internal data class Conversation(
    val takeUntil: (BackendMessage) -> Boolean,
    val sink:      ReplaySink<BackendMessage>,
)

data class PostgresConfig(
    val host:             String,
    val port:             Int    = 5432,
    val user:             String,
    val password:         String,
    val database:         String = user,
    val defaultFetchSize: Int    = 50,
)

class PgConnectionFactory(
    private val config: PostgresConfig,
) : se.oyabun.minamoto.ConnectionFactory {

    private val transport = NettyTransport()
    private val idCounter = java.util.concurrent.atomic.AtomicLong(0)

    override suspend fun create(): se.oyabun.minamoto.Connection {
        val connection = transport.connect(config.host, config.port)
            .await()
            .rightOrThrow()
        val conn = PgConnection(
            id         = ConnectionId(idCounter.incrementAndGet()),
            connection = connection,
            transport  = transport,
        )
        conn.handshake(config.user, config.password, config.database)
        return conn
    }

    override suspend fun validate(connection: se.oyabun.minamoto.Connection): ValidationResult =
        connection.ping()

    override suspend fun destroy(connection: se.oyabun.minamoto.Connection) =
        connection.close()
}
