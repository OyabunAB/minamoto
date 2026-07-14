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
import se.oyabun.aelv.concatMap
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.postgres.PgConnection
import se.oyabun.minamoto.postgres.protocol.BackendMessage.*
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.*

/**
 * Executes a query using the PGwire extended query protocol, streaming [DataRow]s.
 *
 * Uses the unnamed portal and statement — postgres cleans these up automatically
 * at [ReadyForQuery]. Backpressure propagates via [Execute.maxRows]: the server only
 * sends as many rows as requested. [PortalSuspended] triggers subsequent [Execute]
 * messages until all rows are delivered.
 */
internal fun PgConnection.query(
    sql:           String,
    parameters:    List<ByteArray?> = emptyList(),
    statementName: String           = "",
    fetchSize:     Int              = 50,
): Many<DataRow> {
    val portalName = ""

    return Many.defer(factory = suspend {
        exchange(
            messages = listOf(
                Parse(statementName, sql),
                Bind(portalName, statementName, parameters),
                Describe(DescribeTarget.Portal, portalName),
                Execute(portalName, fetchSize),
                Sync,
            ),
            takeUntil = { it is ReadyForQuery },
        ).concatMap { message ->
            when (message) {
                is DataRow         -> Many.items(message)
                is PortalSuspended -> fetchMore(portalName, fetchSize)
                is ErrorResponse   -> Many.error(MinamotoException.QueryFailed(
                    message  = message.message,
                    sqlState = message.sqlState,
                    severity = message.severity,
                    detail   = message.detail,
                    hint     = message.hint,
                ))
                else               -> Many.empty()
            }
        }
    })
}

private fun PgConnection.fetchMore(
    portalName: String,
    fetchSize:  Int,
): Many<DataRow> = Many.defer(factory = suspend {
    exchange(
        messages  = listOf(Execute(portalName, fetchSize), Sync),
        takeUntil = { it is ReadyForQuery || it is PortalSuspended },
    ).concatMap { message ->
        when (message) {
            is DataRow         -> Many.items(message)
            is PortalSuspended -> fetchMore(portalName, fetchSize)
            is ErrorResponse   -> Many.error(MinamotoException.QueryFailed(
                message  = message.message,
                sqlState = message.sqlState,
                severity = message.severity,
                detail   = message.detail,
                hint     = message.hint,
            ))
            else               -> Many.empty()
        }
    }
})
