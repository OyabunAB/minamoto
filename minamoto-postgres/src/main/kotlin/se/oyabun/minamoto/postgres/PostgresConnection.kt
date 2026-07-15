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
import se.oyabun.aelv.netty.readRawByte
import se.oyabun.aelv.netty.upgradeTls
import se.oyabun.aelv.netty.write
import se.oyabun.aelv.publishOn
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import se.oyabun.minamoto.postgres.Parameter
import se.oyabun.minamoto.postgres.protocol.BackendMessage
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NoticeResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NotificationResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ParameterStatus
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ReadyForQuery
import se.oyabun.minamoto.postgres.protocol.FrontendMessage
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Bind
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Close
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Execute
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Parse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Sync
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Terminate
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
internal class PostgresConnection(
    override val id:         ConnectionId,
    internal val connection: NettyConnection,
    private  val transport:  NettyTransport,
    internal val registry:   CodecRegistry    = CodecRegistry(),
    internal val allocator:  ByteBufAllocator = connection.channel.alloc(),
) : Connection {

    private val log = Logging.of<PostgresConnection>()

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
                    Bind("", "", emptyList<Parameter>()),
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

    override suspend fun begin(definition: se.oyabun.minamoto.TransactionDefinition) {
        executeSimple(definition.toBeginSql())
        state = ConnectionState.InTransaction
    }

    override suspend fun commit() {
        executeSimple("COMMIT")
        state = ConnectionState.Idle
    }

    override suspend fun rollback() {
        executeSimple("ROLLBACK")
        state = ConnectionState.Idle
    }

    override suspend fun savepoint(id: se.oyabun.minamoto.SavepointId) =
        executeSimple("SAVEPOINT ${id.value}")

    override suspend fun releaseSavepoint(id: se.oyabun.minamoto.SavepointId) =
        executeSimple("RELEASE SAVEPOINT ${id.value}")

    override suspend fun rollbackToSavepoint(id: se.oyabun.minamoto.SavepointId) =
        executeSimple("ROLLBACK TO SAVEPOINT ${id.value}")

    private suspend fun executeSimple(sql: String) {
        exchange(
            messages = listOf(
                Parse("", sql),
                Bind("", "", emptyList<Parameter>()),
                Execute("", 0),
                Sync,
            ),
            takeUntil = { it is ReadyForQuery },
        ).discard().await()
    }
}

internal data class Conversation(
    val takeUntil: (BackendMessage) -> Boolean,
    val sink:      ReplaySink<BackendMessage>,
)

data class ConnectionConfig(
    val host:             String,
    val port:             Int    = 5432,
    val user:             String,
    val password:         String,
    val database:         String = user,
    val sslMode:          se.oyabun.aelv.netty.SslMode = se.oyabun.aelv.netty.SslMode.Prefer,
    /** Controls the PGwire `Execute.maxRows` per round-trip. 50 means 50 rows per Execute → PortalSuspended → next Execute cycle. Increase for large result sets to reduce round-trips. */
    val defaultFetchSize: Int    = 50,
)

class PostgresConnectionFactory(
    private val config:   ConnectionConfig,
    private val registry: se.oyabun.minamoto.postgres.codec.CodecRegistry = se.oyabun.minamoto.postgres.codec.CodecRegistry(),
) : se.oyabun.minamoto.ConnectionFactory {

    private val transport = NettyTransport()
    private val idCounter = java.util.concurrent.atomic.AtomicLong(0)

    override suspend fun create(): se.oyabun.minamoto.Connection {
        val nettyConnection = transport.connect(config.host, config.port)
            .await()
            .rightOrThrow()

        // TLS negotiation happens at the raw channel level before PostgresConnection
        // wraps the channel — before any framing or subscription is active.
        negotiateSsl(nettyConnection, config.host, config.sslMode)

        val connection = PostgresConnection(
            id         = ConnectionId(idCounter.incrementAndGet()),
            connection = nettyConnection,
            transport  = transport,
            registry   = registry,
        )
        connection.handshake(config.user, config.password, config.database)
        return connection
    }

    override suspend fun validate(connection: se.oyabun.minamoto.Connection): ValidationResult =
        connection.ping()

    override suspend fun destroy(connection: se.oyabun.minamoto.Connection) =
        connection.close()
}

/**
 * Sends PGwire SSLRequest and reads the single-byte server response before any
 * [PostgresConnection] framing is active.
 *
 * Must be called on the raw [NettyConnection] before wrapping it in [PostgresConnection],
 * because [PostgresConnection] subscribes to [inbound][NettyConnection] on construction
 * and would consume the 'S'/'N' byte before [readRawByte] can intercept it.
 */
private suspend fun negotiateSsl(
    connection: NettyConnection,
    host:       String,
    sslMode:    se.oyabun.aelv.netty.SslMode,
) {
    if (sslMode is se.oyabun.aelv.netty.SslMode.Disable) return

    // SSLRequest: 4-byte length (8) + 4-byte magic (80877103)
    val allocator = connection.channel.alloc()
    val buf = allocator.buffer(8)
    buf.writeInt(8)
    buf.writeInt(80877103)
    connection.write(buf).await()

    val response = connection.readRawByte()

    when {
        response == 'S'.code.toByte() ->
            connection.upgradeTls(sslMode, host)
        response == 'N'.code.toByte() && sslMode is se.oyabun.aelv.netty.SslMode.Prefer ->
            Unit // fall back to plain
        else -> throw MinamotoException.TlsFailed(
            "server declined TLS (response: '${response.toInt().toChar()}') but mode $sslMode requires it"
        )
    }
}
