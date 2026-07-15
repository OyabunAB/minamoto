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
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.postgres.Column
import se.oyabun.minamoto.postgres.Parameter
import se.oyabun.minamoto.postgres.Parameters
import se.oyabun.minamoto.postgres.PostgresConnection
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
 * Phase 1: Parse + Describe(Statement) + Sync — learns column OIDs and preferred formats.
 * Phase 2: Bind (with per-column result formats from the codec registry) + Execute + Sync.
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

    return Many.defer(factory = suspend {
        exchange(
            messages  = listOf(Parse("", statement), Describe(DescribeTarget.Statement, ""), Sync),
            takeUntil = { it is ReadyForQuery },
        )
    }).concatMap { message ->
        when (message) {
            is RowDescription -> Many.items(message.columns)
            is ErrorResponse  -> Many.error(message.asException())
            else              -> Many.empty()
        }
    }.fold(emptyList<Column.Description>()) { _, columns -> columns }
     .flatMapMany(transform = suspend { descriptions: List<Column.Description> ->
         val resultFormats = descriptions.map { registry.preferredFormat(it.typeOid).wire }
             .distinct()
             .let { if (it.size == 1) listOf(it.first()) else descriptions.map { col -> registry.preferredFormat(col.typeOid).wire } }

         exchange(
             messages  = listOf(
                 Bind(portalName, "", parameters, resultFormats = resultFormats),
                 Execute(portalName, fetchSize),
                 Sync,
             ),
             takeUntil = { it is ReadyForQuery },
         ).concatMap { message ->
             when (message) {
                 is DataRow         -> Many.items(buildRow(message, descriptions))
                 is PortalSuspended -> fetchMore(portalName, fetchSize, descriptions, parameters)
                 is ErrorResponse   -> Many.error(message.asException())
                 else               -> Many.empty()
             }
         }
     })
}

/**
 * Executes a command and returns the number of rows affected from [CommandComplete].
 *
 * The tag format is "INSERT 0 n", "UPDATE n", "DELETE n", "SELECT n", etc.
 * The affected count is the last whitespace-delimited token.
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

private fun ErrorResponse.asException(): MinamotoException = when (sqlState) {
    "23505" -> MinamotoException.UniqueViolation(message, sqlState, detail, hint)
    "23503" -> MinamotoException.ForeignKeyViolation(message, sqlState, detail, hint)
    "23502" -> MinamotoException.NotNullViolation(message, sqlState, detail)
    "23514" -> MinamotoException.CheckViolation(message, sqlState, detail)
    "40001" -> MinamotoException.SerializationFailure(message)
    "40P01" -> MinamotoException.ServerDeadlockDetected(message)
    "57014" -> MinamotoException.QueryCancelled(message)
    "42601" -> MinamotoException.SyntaxError(message, detail)
    "42P01" -> MinamotoException.UndefinedTable(message)
    "42703" -> MinamotoException.UndefinedColumn(message)
    else    -> MinamotoException.QueryFailed(
        message  = message,
        sqlState = sqlState,
        severity = severity,
        detail   = detail,
        hint     = hint,
    )
}
