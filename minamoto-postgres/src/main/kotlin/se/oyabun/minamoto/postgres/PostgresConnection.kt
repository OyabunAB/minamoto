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
import se.oyabun.aelv.One
import se.oyabun.aelv.None
import se.oyabun.aelv.ReplaySink
import se.oyabun.aelv.await
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.drain
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapNone
import se.oyabun.aelv.fold
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.map
import se.oyabun.aelv.or
import se.oyabun.aelv.andThen
import se.oyabun.aelv.netty.ChannelBinding
import se.oyabun.aelv.netty.NettyConnection
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.netty.SslMode
import se.oyabun.aelv.netty.TcpOptions
import se.oyabun.aelv.netty.channelBinding
import se.oyabun.aelv.netty.upgradeTls
import se.oyabun.aelv.netty.inbound
import se.oyabun.aelv.netty.readRawByte
import se.oyabun.aelv.netty.upgradeTls
import se.oyabun.aelv.netty.write
import se.oyabun.aelv.publishOn
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.Connection
import se.oyabun.minamoto.ConnectionFactory
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.SavepointId
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import se.oyabun.minamoto.postgres.Parameter
import se.oyabun.minamoto.postgres.protocol.BackendMessage
import se.oyabun.minamoto.postgres.protocol.BackendMessage.KeyData
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NoticeResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NotificationResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ParameterStatus
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ReadyForQuery
import se.oyabun.minamoto.postgres.protocol.FrontendMessage
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Bind
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.CancelRequest
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
    internal val statementCache: PreparedStatementCache = PreparedStatementCache(0),
) : Connection {

    private val log = Logging.of<PostgresConnection>()

    override var state: ConnectionState = ConnectionState.Idle
        private set

    /** Server session parameters received via [ParameterStatus] messages. */
    val serverParameters: java.util.concurrent.ConcurrentHashMap<String, String> = java.util.concurrent.ConcurrentHashMap()

    /** Backend key data received during handshake — used to send [CancelRequest] on a separate connection. */
    internal var backendKeyData: KeyData? = null

    /** Receives [NotificationResponse] messages when this connection is in LISTEN mode. Null when not listening. */
    internal var notificationHandler: ((BackendMessage.NotificationResponse) -> Unit)? = null

    /** Allocates the next unique portal name for a streaming query on this connection. */
    internal fun nextPortalName(): String = "p_${portalCounter.incrementAndGet()}"

    internal val writeMutex    = Mutex()
    private  val portalCounter = java.util.concurrent.atomic.AtomicLong(0)
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
                log.protocol.messageReceived(id, when (message) {
                    is BackendMessage.ReadyForQuery  -> "ReadyForQuery tx=${message.transactionStatus::class.simpleName}"
                    is BackendMessage.CommandComplete -> "CommandComplete ${message.tag}"
                    is BackendMessage.DataRow         -> "DataRow cols=${message.values.size}"
                    else                              -> message::class.simpleName ?: "Unknown"
                })
                when (message) {
                    is NoticeResponse       -> {
                        log.connection.notice(id, message.severity, message.message)
                        return@drain
                    }
                    is NotificationResponse -> {
                        notificationHandler?.invoke(message)
                        return@drain
                    }
                    is ParameterStatus      -> {
                        serverParameters[message.name] = message.value
                        log.protocol.parameterStatus(id, message.name, message.value)
                        return@drain
                    }
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
                    log.protocol.conversationComplete(id, conversations.size)
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
     * Registers a conversation, sends [messages], and returns the response stream.
     *
     * Cold — the mutex acquisition and wire writes happen at subscription time, not
     * at call time. This means the pipeline can be defined without a coroutine context
     * and the actual I/O deferred until the pipeline is subscribed.
     */
    fun exchange(
        messages:  List<FrontendMessage>,
        takeUntil: (BackendMessage) -> Boolean,
    ): Many<BackendMessage> = Many.defer(factory = suspend {
        val sink         = ReplaySink<BackendMessage>()
        val conversation = Conversation(takeUntil, sink)

        writeMutex.withLock {
            conversations.add(conversation)
            log.protocol.conversationQueued(id, conversations.size)
            messages.forEach { msg ->
                log.protocol.messageSent(id, when (msg) {
                    is FrontendMessage.Parse    -> "Parse ${msg.statementName.ifEmpty { "<unnamed>" }} ${msg.statement.take(80)}"
                    is FrontendMessage.Execute  -> "Execute maxRows=${msg.maxRows}"
                    else                        -> msg::class.simpleName ?: "Unknown"
                })
                connection.write(MessageEncoder.encode(msg, allocator)).await()
            }
        }

        sink.asMany()
    })

    override fun ping(): One<ValidationResult> =
        exchange(
            messages  = listOf(Parse("", "SELECT 1"), Bind("", "", emptyList<Parameter>()), Execute("", 1), Sync),
            takeUntil = { it is ReadyForQuery },
        ).fold(ValidationResult.Valid as ValidationResult) { acc, msg ->
            if (msg is BackendMessage.ErrorResponse) ValidationResult.Invalid(msg.message) else acc
        }

    override fun close(): None<Unit> =
        None.defer<Unit> { log.connection.closing(id) { state = ConnectionState.Closing; subscription.cancel() } }
            .andThen { connection.write(MessageEncoder.encode(Terminate, allocator)) }
            .andThen { None.defer<Unit> { connection.channel.close().sync(); log.connection.closed(id) { state = ConnectionState.Closed } } }

    internal fun executeSimpleCommand(sql: String): None<Unit> = executeSimple(sql)

    private fun executeSimple(sql: String): None<Unit> =
        exchange(
            messages  = listOf(Parse("", sql), Bind("", "", emptyList<Parameter>()), Execute("", 0), Sync),
            takeUntil = { it is ReadyForQuery },
        ).discard().andThen { None.complete<Unit>() }

    override fun begin(definition: TransactionDefinition): None<Unit> =
        executeSimple(definition.toBeginSql())
            .andThen { None.defer<Unit> { state = ConnectionState.InTransaction } }

    override fun commit(): None<Unit> =
        executeSimple("COMMIT")
            .andThen { None.defer<Unit> { state = ConnectionState.Idle } }

    override fun rollback(): None<Unit> =
        executeSimple("ROLLBACK")
            .andThen { None.defer<Unit> { state = ConnectionState.Idle } }

    override fun savepoint(id: SavepointId): None<Unit> = executeSimple("SAVEPOINT ${id.value}")
    override fun releaseSavepoint(id: SavepointId): None<Unit> = executeSimple("RELEASE SAVEPOINT ${id.value}")
    override fun rollbackToSavepoint(id: SavepointId): None<Unit> = executeSimple("ROLLBACK TO SAVEPOINT ${id.value}")

    internal fun queryBoolean(sql: String): One<Boolean> =
        exchange(
            messages = listOf(
                Parse("", sql),
                Bind("", "", emptyList<Parameter>()),
                Execute("", 1),
                Sync,
            ),
            takeUntil = { it is ReadyForQuery },
        ).fold(false) { acc, msg ->
            if (msg is BackendMessage.DataRow) {
                val value = msg.values.firstOrNull()?.firstOrNull()
                value == 1.toByte() || value == 't'.code.toByte()
            } else acc
        }

    /**
     * Sends a [CancelRequest] for any query currently executing on this connection.
     *
     * Opens a fresh TCP connection to the server, sends the cancel, and closes it.
     * This is a best-effort signal — the server may have already finished the query
     * by the time the request arrives.
     */
    override fun cancel(): None<Unit> {
        val keyData = backendKeyData ?: return None.complete()
        val address = connection.channel.remoteAddress() as? java.net.InetSocketAddress
            ?: return None.complete()
        return transport.connect(address.hostString, address.port)
            .flatMapNone { cancelConnection ->
                cancelConnection.write(MessageEncoder.encode(CancelRequest(keyData.processId, keyData.secretKey), allocator))
                    .andThen { None.defer<Unit> { cancelConnection.channel.close().sync() } }
            }
            .recover {
                None.defer { log.connection.invalidState(id, "cancel request failed — server may have already completed the query") }
            }
            .andThen { None.complete<Unit>() }
    }
}

internal data class Conversation(
    val takeUntil: (BackendMessage) -> Boolean,
    val sink:      ReplaySink<BackendMessage>,
)

data class ConnectionConfig(
    /**
     * Host list in priority order. The first reachable host matching [hostSelectionStrategy]
     * is used. Single-host setups use a list of one.
     */
    val hosts:                           Hosts                                 = emptyList(),
    /**
     * Convenience single-host constructor fields — used when [hosts] is empty.
     * Ignored if [hosts] is non-empty.
     */
    val host:                            String                                = "localhost",
    val port:                            Int                                   = 5432,
    val user:                            String,
    /** Supplies the password for each new physical connection. Called once per [PostgresConnectionFactory.create]. */
    val password:                        CredentialSupplier,
    val database:                        String                                = user,
    val sslMode:                         SslMode         = SslMode.Prefer,
    val tcpOptions:                      TcpOptions      = TcpOptions(),
    val applicationName:                 String                                = "minamoto",
    val hostSelectionStrategy:           HostSelectionStrategy                 = HostSelectionStrategy.Any,
    /** Schema search order — sent as `search_path`. Empty means server default. */
    val searchPath:                      List<String>                          = emptyList(),
    /** Session timezone — sent as `timezone`. Null means server default. */
    val timezone:                        String?                               = null,
    /** Aborts any statement taking longer than this — sent as `statement_timeout` in milliseconds. */
    val statementTimeout:                kotlin.time.Duration?                 = null,
    /** Aborts waiting for a lock after this duration — sent as `lock_timeout` in milliseconds. */
    val lockTimeout:                     kotlin.time.Duration?                 = null,
    /** Terminates sessions idle inside a transaction after this duration — sent as `idle_in_transaction_session_timeout`. */
    val idleInTransactionSessionTimeout: kotlin.time.Duration?                 = null,
    /** Unix domain socket path. When set, [host] and [port] are ignored and connection goes via the socket. */
    val unixSocketPath:                  String?                               = null,
    /** Controls the PGwire `Execute.maxRows` per round-trip. 50 means 50 rows per Execute → PortalSuspended → next Execute cycle. Increase for large result sets to reduce round-trips. */
    val defaultFetchSize:                Int                                   = 50,
    /**
     * Maximum number of named prepared statements cached per physical connection.
     * Cached statements skip the `Parse` + `Describe` round-trip on repeated execution.
     * Set to 0 to disable caching — every execution re-parses.
     */
    val statementCacheSize:              Int                                   = 256,
) {
    /** Resolved host list — [hosts] if non-empty, otherwise the single [host]/[port] pair. */
    fun resolvedHosts(): List<Host> =
        hosts.ifEmpty { listOf(Host(host, port)) }
}

class PostgresConnectionFactory(
    private val config:   ConnectionConfig,
    private val registry: CodecRegistry = CodecRegistry(),
) : ConnectionFactory {

    private val transport = NettyTransport()
    private val idCounter = java.util.concurrent.atomic.AtomicLong(0)

    override fun create(): One<Connection> =
        Many.from(config.resolvedHosts())
            .concatMap { host -> tryCreate(host).toMany().recover { Many.empty() } }
            .firstMaybe()
            .or { throw DatabaseException.ConnectionLost(
                "all hosts exhausted — tried: ${config.resolvedHosts().joinToString { "${it.hostname}:${it.port}" }}"
            ) }

    private fun tryCreate(host: Host): One<Connection> =
        (if (config.unixSocketPath != null)
            transport.connectUnix(config.unixSocketPath!!, config.tcpOptions)
        else
            transport.connect(host.hostname, host.port, config.tcpOptions))
        .flatMap { nettyConnection ->
            negotiateSsl(nettyConnection, host.hostname, config.sslMode)
                .map { channelBinding -> nettyConnection to channelBinding }
        }
        .flatMap { (nettyConnection, channelBinding) ->
            val connection = PostgresConnection(
                id             = ConnectionId(idCounter.incrementAndGet()),
                connection     = nettyConnection,
                transport      = transport,
                registry       = registry,
                statementCache = PreparedStatementCache(config.statementCacheSize),
            )
            connection.handshake(
                user                            = config.user,
                password                        = config.password(),
                database                        = config.database,
                applicationName                 = config.applicationName,
                searchPath                      = config.searchPath,
                timezone                        = config.timezone,
                statementTimeout                = config.statementTimeout,
                lockTimeout                     = config.lockTimeout,
                idleInTransactionSessionTimeout = config.idleInTransactionSessionTimeout,
                channelBinding                  = channelBinding,
            ).map { keyData -> connection.backendKeyData = keyData; connection }
        }
        .flatMap { connection ->
            checkHostRole(connection, host).thenReturn(connection as Connection)
        }

    private fun checkHostRole(connection: PostgresConnection, host: Host): None<Boolean> =
        when (config.hostSelectionStrategy) {
            HostSelectionStrategy.Any -> None.complete()
            HostSelectionStrategy.Primary ->
                connection.queryBoolean("SELECT NOT pg_is_in_recovery()")
                    .flatMapNone { isPrimary ->
                        if (!isPrimary) None.error<Boolean>(DatabaseException.InvalidState("${host.hostname}:${host.port} is not a primary"))
                        else None.complete<Boolean>()
                    }
            HostSelectionStrategy.Secondary ->
                connection.queryBoolean("SELECT pg_is_in_recovery()")
                    .flatMapNone { isSecondary ->
                        if (!isSecondary) None.error<Boolean>(DatabaseException.InvalidState("${host.hostname}:${host.port} is not a secondary"))
                        else None.complete<Boolean>()
                    }
        }

    override fun validate(connection: Connection): One<ValidationResult> =
        connection.ping()

    override fun destroy(connection: Connection): None<Unit> =
        connection.close()

    fun close(): None<Unit> = transport.close().discard()
}

/**
 * Sends PGwire SSLRequest and reads the single-byte server response before any
 * [PostgresConnection] framing is active.
 *
 * Must be called on the raw [NettyConnection] before wrapping it in [PostgresConnection],
 * because [PostgresConnection] subscribes to [inbound][NettyConnection] on construction
 * and would consume the 'S'/'N' byte before [readRawByte] can intercept it.
 */
private fun negotiateSsl(connection: NettyConnection, host: String, sslMode: SslMode): One<ChannelBinding> {
    if (sslMode is SslMode.Disable) return One.single(ChannelBinding.None)
    val allocator = connection.channel.alloc()
    val buf = allocator.buffer(8).also { it.writeInt(8); it.writeInt(80877103) }
    return connection.write(buf)
        .andThen { One.defer { connection.readRawByte() } }
        .flatMap { response ->
            when {
                response == 'S'.code.toByte() ->
                    One.defer<ChannelBinding> {
                        connection.upgradeTls(sslMode, host)
                        connection.channelBinding()
                    }
                response == 'N'.code.toByte() && sslMode is SslMode.Prefer ->
                    One.single(ChannelBinding.None)
                else -> One.error<ChannelBinding>(DatabaseException.TlsFailed(
                    "server declined TLS (response: '${response.toInt().toChar()}') but mode $sslMode requires it"
                ))
            }
        }
}
