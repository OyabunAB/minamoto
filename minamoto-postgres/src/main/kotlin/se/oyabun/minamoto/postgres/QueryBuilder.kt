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

import kotlinx.coroutines.currentCoroutineContext
import se.oyabun.aelv.Many
import se.oyabun.aelv.Maybe
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.flatMapNone
import se.oyabun.aelv.or
import se.oyabun.aelv.resource
import se.oyabun.minamoto.Binding
import se.oyabun.minamoto.BoundModify
import se.oyabun.minamoto.BoundRun
import se.oyabun.minamoto.BoundQuery
import se.oyabun.minamoto.ModifyBuilder
import se.oyabun.minamoto.ConnectionAcquireResult
import se.oyabun.minamoto.ConnectionContext
import se.oyabun.minamoto.RunBuilder
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.QueryBuilder
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.pool.ManagedPool
import se.oyabun.minamoto.pool.AcquireResult
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import se.oyabun.minamoto.postgres.protocol.executeCommand
import se.oyabun.minamoto.postgres.protocol.executeEffect
import se.oyabun.minamoto.postgres.protocol.executeQuery

private val log = Logging.of<PostgresBoundQuery>()

/**
 * A query with named parameters, ready to be bound.
 *
 * Named parameters use `:name` syntax. [bind] rewrites the SQL to Postgres positional
 * parameters (`$1`, `$2`, ...) at subscription time, using the codec registry from the
 * resolved connection.
 */
class PostgresQuery internal constructor(
    private val registry:  CodecRegistry,
    private val statement: String,
) : QueryBuilder {
    override fun bind(vararg bindings: Binding): PostgresBoundQuery =
        PostgresBoundQuery(registry, statement, bindings.toList())
}

/**
 * A command (INSERT / UPDATE / DELETE / DDL) with named parameters, ready to be bound.
 */
class PostgresModify internal constructor(
    private val registry:  CodecRegistry,
    private val statement: String,
) : ModifyBuilder {
    override fun bind(vararg bindings: Binding): PostgresBoundModify =
        PostgresBoundModify(registry, statement, bindings.toList())
}

class PostgresBoundQuery internal constructor(
    private val registry:  CodecRegistry,
    private val statement: String,
    private val bindings:  List<Binding>,
) : BoundQuery {

    override fun multiple(): Many<Row> =
        withConnection(registry) { connection ->
            val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings, connection)
            connection.executeQuery(rewrittenSql, parameters)
        }

    override fun single(): One<Row> =
        withConnection(registry) { connection ->
            val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings, connection)
            connection.executeQuery(rewrittenSql, parameters)
        }.firstMaybe().or { throw DatabaseException.QueryFailed(
            message  = "expected at least one row but got none",
            sqlState = "02000",
            severity = "ERROR",
        )}

    override fun optional(): Maybe<Row> =
        withConnection(registry) { connection ->
            val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings, connection)
            connection.executeQuery(rewrittenSql, parameters)
        }.firstMaybe()
}

class PostgresBoundModify internal constructor(
    private val registry:  CodecRegistry,
    private val statement: String,
    private val bindings:  List<Binding>,
) : BoundModify {

    override fun count(): One<Long> =
        withConnectionOne(registry) { connection ->
            val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings, connection)
            connection.executeCommand(rewrittenSql, parameters)
        }
}

class PostgresRun internal constructor(
    private val registry:  CodecRegistry,
    private val statement: String,
) : RunBuilder {
    override fun bind(vararg bindings: Binding): PostgresBoundRun =
        PostgresBoundRun(registry, statement, bindings.toList())
}

class PostgresBoundRun internal constructor(
    private val registry:  CodecRegistry,
    private val statement: String,
    private val bindings:  List<Binding>,
) : BoundRun {
    override fun execute(): None<Unit> =
        withConnectionNone(registry) { connection ->
            val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings, connection)
            connection.executeEffect(rewrittenSql, parameters)
        }
}

/**
 * Resolves a [PostgresConnection] and runs [block] over it as a [Many] pipeline.
 *
 * If a transaction is active in the coroutine context, the transaction's connection is used
 * directly — no acquire or release. Otherwise, a connection is borrowed from the [PoolContext]
 * pool and released when the pipeline terminates (normally, error, or cancellation).
 */
private fun resolveSlot(pool: ManagedPool): One<Pair<PostgresConnection, ManagedPool?>> =
    pool.acquireSlot().flatMap { result ->
        when (result) {
            is AcquireResult.Acquired          -> One.single(result.slot.connection as PostgresConnection to (pool as ManagedPool?))
            is AcquireResult.TimedOut          -> One.error(DatabaseException.AcquireTimeout(pool.config.acquireTimeout))
            is AcquireResult.DeadlockPrevented -> One.error(DatabaseException.DeadlockDetected(result.held, result.poolSize))
        }
    }.doOnNext { (conn: PostgresConnection, _: ManagedPool?) -> log.query.reusingConnection(conn.id) }

private fun withConnection(
    @Suppress("UNUSED_PARAMETER") registry: CodecRegistry,
    block: (PostgresConnection) -> Many<Row>,
): Many<Row> = Many.defer(factory = suspend {
    val context           = currentCoroutineContext()
    val connectionContext = context[ConnectionContext]
    val activeId          = connectionContext?.activeConnectionId()
    if (activeId != null) {
        log.query.reusingConnection(activeId)
        val pool = context[PoolContext]?.pool as? ManagedPool
            ?: return@defer Many.error(DatabaseException.InvalidState("connection active in context but no ManagedPool found"))
        val connection = pool.connectionFor(activeId) as? PostgresConnection
            ?: return@defer Many.error(DatabaseException.InvalidState("active connection ${activeId.value} not found in pool"))
        block(connection)
    } else {
        log.query.acquiringConnection()
        val pool = context[PoolContext]?.pool as? ManagedPool
            ?: return@defer Many.error(DatabaseException.InvalidState("no pool in context — use pool { } or pool.transactionally { }"))
        Many.resource(
            acquire = { resolveSlot(pool) },
            release = { (connection, pool), _ -> if (pool != null) pool.release(connection.id) else None.complete<Nothing>() },
            use     = { (connection, _) -> block(connection) },
        )
    }
})

private fun withConnectionOne(
    @Suppress("UNUSED_PARAMETER") registry: CodecRegistry,
    block: (PostgresConnection) -> One<Long>,
): One<Long> = Many.defer(factory = suspend {
    val context           = currentCoroutineContext()
    val connectionContext = context[ConnectionContext]
    val activeId          = connectionContext?.activeConnectionId()
    if (activeId != null) {
        log.query.reusingConnection(activeId)
        val pool = context[PoolContext]?.pool as? ManagedPool
            ?: return@defer Many.error(DatabaseException.InvalidState("connection active in context but no ManagedPool found"))
        val connection = pool.connectionFor(activeId) as? PostgresConnection
            ?: return@defer Many.error(DatabaseException.InvalidState("active connection ${activeId.value} not found in pool"))
        block(connection).toMany()
    } else {
        log.query.acquiringConnection()
        val pool = context[PoolContext]?.pool as? ManagedPool
            ?: return@defer Many.error(DatabaseException.InvalidState("no pool in context — use pool { } or pool.transactionally { }"))
        One.resource(
            acquire = { resolveSlot(pool) },
            release = { (connection, pool), _ -> if (pool != null) pool.release(connection.id) else None.complete<Nothing>() },
            use     = { (connection, _) -> block(connection) },
        ).toMany()
    }
}).firstMaybe().or { throw DatabaseException.InvalidState("modify returned no count") }

private fun withConnectionNone(
    @Suppress("UNUSED_PARAMETER") registry: CodecRegistry,
    block: (PostgresConnection) -> None<Unit>,
): None<Unit> = Many.defer(factory = suspend {
    val context           = currentCoroutineContext()
    val connectionContext = context[ConnectionContext]
    val activeId          = connectionContext?.activeConnectionId()
    if (activeId != null) {
        log.query.reusingConnection(activeId)
        val pool = context[PoolContext]?.pool as? ManagedPool
            ?: return@defer Many.error(DatabaseException.InvalidState("connection active in context but no ManagedPool found"))
        val connection = pool.connectionFor(activeId) as? PostgresConnection
            ?: return@defer Many.error(DatabaseException.InvalidState("active connection ${activeId.value} not found in pool"))
        block(connection).toMany()
    } else {
        log.query.acquiringConnection()
        val pool = context[PoolContext]?.pool as? ManagedPool
            ?: return@defer Many.error(DatabaseException.InvalidState("no pool in context — use pool { } or pool.transactionally { }"))
        None.resource(
            acquire = { resolveSlot(pool) },
            release = { (connection, pool), _ -> if (pool != null) pool.release(connection.id) else None.complete<Nothing>() },
            use     = { (connection, _) -> block(connection) },
        ).toMany()
    }
}).discard()

/**
 * Rewrites `:name` parameters to Postgres `$n` positional parameters and encodes values.
 *
 * Returns the rewritten SQL and the [Parameter] list in positional order.
 * Skips rewriting inside single-quoted literals, dollar-quoted strings, and comments.
 */
internal fun rewriteAndEncode(
    sql:        String,
    bindings:   List<Binding>,
    connection: PostgresConnection,
): Pair<String, Parameters> {
    val bindingMap  = bindings.toMap()
    val nameToIndex = mutableMapOf<String, Int>()
    val parameters  = mutableListOf<Parameter>()
    val result      = StringBuilder()
    var index       = 0

    while (index < sql.length) {
        when {
            sql[index] == '\'' -> {
                result.append(sql[index++])
                while (index < sql.length) {
                    val char = sql[index++]
                    result.append(char)
                    if (char == '\'' && (index >= sql.length || sql[index] != '\'')) break
                }
            }

            sql[index] == '$' && index + 1 < sql.length && (sql[index + 1] == '$' || sql[index + 1].isLetter()) -> {
                val tagEnd   = sql.indexOf('$', index + 1)
                if (tagEnd == -1) { result.append(sql[index++]); continue }
                val tag      = sql.substring(index, tagEnd + 1)
                val closeTag = sql.indexOf(tag, tagEnd + 1)
                if (closeTag == -1) { result.append(sql[index++]); continue }
                result.append(sql.substring(index, closeTag + tag.length))
                index = closeTag + tag.length
            }

            sql[index] == '-' && index + 1 < sql.length && sql[index + 1] == '-' -> {
                val eol = sql.indexOf('\n', index).let { if (it == -1) sql.length else it + 1 }
                result.append(sql.substring(index, eol))
                index = eol
            }

            sql[index] == '/' && index + 1 < sql.length && sql[index + 1] == '*' -> {
                val close = sql.indexOf("*/", index + 2).let { if (it == -1) sql.length else it + 2 }
                result.append(sql.substring(index, close))
                index = close
            }

            sql[index] == ':' && index + 1 < sql.length && sql[index + 1] == ':' -> {
                result.append("::")
                index += 2
            }

            sql[index] == ':' && index + 1 < sql.length && sql[index + 1].isLetter() -> {
                val nameStart = index + 1
                var nameEnd   = nameStart
                while (nameEnd < sql.length && (sql[nameEnd].isLetterOrDigit() || sql[nameEnd] == '_')) nameEnd++
                val name      = sql.substring(nameStart, nameEnd)
                val position  = nameToIndex.getOrPut(name) {
                    parameters.add(encodeBinding(name, bindingMap[name], connection))
                    parameters.size
                }
                result.append("\$$position")
                index = nameEnd
            }

            else -> result.append(sql[index++])
        }
    }

    return Pair(result.toString(), parameters)
}

/**
 * Encodes a named binding to a [Parameter].
 *
 * Passing `null` for [value] throws [DatabaseException.InvalidState] — use [Parameter.Undefined] to bind SQL NULL.
 */
private fun encodeBinding(name: String, value: Any?, connection: PostgresConnection): Parameter {
    if (value == null) throw DatabaseException.InvalidState(
        "no binding provided for parameter :$name — use Parameter.Undefined for SQL NULL"
    )
    if (value is Parameter.Undefined) return Parameter.Undefined
    val codec        = connection.registry.findForEncoding(value)
    val (bytes, fmt) = codec.encode(value)
    return Parameter.Defined(bytes, fmt)
}
