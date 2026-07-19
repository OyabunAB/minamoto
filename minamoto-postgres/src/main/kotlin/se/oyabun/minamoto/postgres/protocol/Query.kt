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
package se.oyabun.minamoto.postgres.protocol

import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.discard
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.fold
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.postgres.Column
import se.oyabun.minamoto.postgres.Parameter
import se.oyabun.minamoto.postgres.Parameters
import se.oyabun.minamoto.postgres.PostgresConnection
import se.oyabun.minamoto.postgres.PreparedStatementCache
import se.oyabun.minamoto.postgres.PostgresRow
import se.oyabun.minamoto.postgres.protocol.BackendMessage.CommandComplete
import se.oyabun.minamoto.postgres.protocol.BackendMessage.DataRow
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ErrorResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.PortalSuspended
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ReadyForQuery
import se.oyabun.minamoto.postgres.protocol.BackendMessage.RowDescription
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Bind
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Describe
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Execute
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Parse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Sync

/**
 * Executes a query using the PGwire extended query protocol, streaming typed [Row]s.
 *
 * When [PostgresConnection.statementCache] is enabled and [statement] has been executed
 * before, the `Parse` + `Describe` round-trip is skipped and the cached statement name
 * and column descriptions are reused directly. On first execution the statement is
 * parsed under a generated name and cached.
 *
 * Backpressure propagates via [Execute.maxRows]. [PortalSuspended] triggers subsequent
 * [Execute] messages until all rows are delivered.
 */
internal fun PostgresConnection.executeQuery(
    statement:  String,
    parameters: Parameters = emptyList(),
    fetchSize:  Int        = 50,
): Many<Row> {
    val portalName = ""
    return parseAndDescribe(statement)
        .fold(emptyList<Column.Description>()) { acc, col -> acc + col }
        .flatMapMany(transform = suspend { descriptions: List<Column.Description> ->
            val statementName = statementCache.get(statement)?.name ?: ""
            val resultFormats = descriptions.map { registry.preferredFormat(it.typeOid).wire }
                .distinct()
                .let { formats ->
                    if (formats.size == 1) listOf(formats.first())
                    else descriptions.map { col -> registry.preferredFormat(col.typeOid).wire }
                }

            exchange(
                messages  = listOf(
                    Bind(portalName, statementName, parameters, resultFormats = resultFormats),
                    Execute(portalName, fetchSize),
                    Sync,
                ),
                takeUntil = { it is ReadyForQuery },
            ).concatMap { message ->
                when (message) {
                    is DataRow         -> Many.items(buildRow(message, descriptions))
                    is PortalSuspended -> fetchMore(portalName, fetchSize, descriptions, parameters)
                    is ErrorResponse   -> {
                        if (message.sqlState == INVALID_PREPARED_STATEMENT) {
                            statementCache.evict(statement)
                            executeQuery(statement, parameters, fetchSize)
                        } else {
                            Many.error(message.asException())
                        }
                    }
                    else               -> Many.empty()
                }
            }
        })
}

/**
 * Executes a DML command and returns the number of affected rows from [CommandComplete].
 */
internal fun PostgresConnection.executeCommand(
    statement:  String,
    parameters: Parameters = emptyList(),
): One<Long> = Many.defer(factory = suspend {
    exchange(
        messages = listOf(
            Parse("", statement),
            Bind("", "", parameters),
            Describe(DescribeTarget.Portal, ""),
            Execute("", 0),
            Sync,
        ),
        takeUntil = { it is ReadyForQuery },
    )
}).fold(0L) { rowsAffected, message ->
    when (message) {
        is CommandComplete -> message.tag.split(" ").last().toLongOrNull() ?: rowsAffected
        is ErrorResponse   -> throw message.asException()
        else               -> rowsAffected
    }
}

internal fun PostgresConnection.executeEffect(
    statement:  String,
    parameters: Parameters = emptyList(),
): None<Unit> = Many.defer(factory = suspend {
    exchange(
        messages = listOf(
            Parse("", statement),
            Bind("", "", parameters),
            Describe(DescribeTarget.Portal, ""),
            Execute("", 0),
            Sync,
        ),
        takeUntil = { it is ReadyForQuery },
    )
}).fold(Unit) { _, message ->
    if (message is ErrorResponse) throw message.asException()
}.discard()

/**
 * Returns column descriptions for [statement], using the cache when available.
 *
 * On a cache hit, returns the stored [Column.Description]s immediately without a round-trip.
 * On a cache miss, generates a statement name, sends `Parse` + `Describe` + `Sync`, caches
 * the result, and emits the [Column.Description]s from the server's [RowDescription].
 * Any evicted statement is closed on the server before the new `Parse` is sent.
 */
private fun PostgresConnection.parseAndDescribe(statement: String): Many<Column.Description> {
    val cached = statementCache.get(statement)
    if (cached != null) return Many.items(*cached.descriptions.toTypedArray())

    val name = statementCache.nextName()
    statementCache.put(statement, PreparedStatementCache.Entry(name, emptyList()))

    return Many.defer(factory = suspend {
        exchange(
            messages  = if (name.isEmpty())
                listOf(Parse("", statement), Describe(DescribeTarget.Statement, ""), Sync)
            else
                listOf(Parse(name, statement), Describe(DescribeTarget.Statement, name), Sync),
            takeUntil = { it is ReadyForQuery },
        )
    }).concatMap { message ->
        when (message) {
            is RowDescription -> {
                if (name.isNotEmpty()) statementCache.put(statement, PreparedStatementCache.Entry(name, message.columns))
                Many.items(*message.columns.toTypedArray())
            }
            is ErrorResponse  -> {
                statementCache.evict(statement)
                Many.error(message.asException())
            }
            else              -> Many.empty()
        }
    }
}

private fun PostgresConnection.fetchMore(
    portalName:   String,
    fetchSize:    Int,
    descriptions: List<Column.Description>,
    parameters:   Parameters,
): Many<Row> = Many.defer(factory = suspend {
    exchange(
        messages  = listOf(Execute(portalName, fetchSize), Sync),
        takeUntil = { it is ReadyForQuery || it is PortalSuspended },
    )
}).concatMap { message ->
    when (message) {
        is DataRow         -> Many.items(buildRow(message, descriptions))
        is PortalSuspended -> fetchMore(portalName, fetchSize, descriptions, parameters)
        is ErrorResponse   -> Many.error(message.asException())
        else               -> Many.empty()
    }
}

private fun PostgresConnection.buildRow(
    dataRow:      DataRow,
    descriptions: List<Column.Description>,
): PostgresRow {
    val columns = descriptions.zip(dataRow.values) { description, bytes ->
        Column(
            description = description,
            value       = if (bytes == null) Column.Value.Missing
                          else Column.Value.Present(bytes),
        )
    }
    return PostgresRow(columns, registry)
}

private fun ErrorResponse.asException(): DatabaseException = when (sqlState) {
    "23505" -> DatabaseException.UniqueViolation(message, sqlState, detail, hint)
    "23503" -> DatabaseException.ForeignKeyViolation(message, sqlState, detail, hint)
    "23502" -> DatabaseException.NotNullViolation(message, sqlState, detail)
    "23514" -> DatabaseException.CheckViolation(message, sqlState, detail)
    "40001" -> DatabaseException.SerializationFailure(message)
    "40P01" -> DatabaseException.ServerDeadlockDetected(message)
    "57014" -> DatabaseException.QueryCancelled(message)
    "42601" -> DatabaseException.SyntaxError(message, detail)
    "42P01" -> DatabaseException.UndefinedTable(message)
    "42703" -> DatabaseException.UndefinedColumn(message)
    "42501" -> DatabaseException.PermissionDenied(message)
    else    -> DatabaseException.QueryFailed(
        message  = message,
        sqlState = sqlState,
        severity = severity,
        detail   = detail,
        hint     = hint,
    )
}

private const val INVALID_PREPARED_STATEMENT = "26000"
