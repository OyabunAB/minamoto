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

import se.oyabun.minamoto.postgres.Column
import se.oyabun.minamoto.postgres.Parameter
import se.oyabun.minamoto.postgres.Parameters

internal sealed interface FrontendMessage {

    /**
     * Initiates a connection. Sent once before authentication, carrying all session
     * parameters as startup key-value pairs. Parameters set here take effect for the
     * lifetime of the session without requiring a subsequent `SET` command.
     *
     * To change a parameter after connect, issue an explicit `SET` SQL command;
     * the server will send a [BackendMessage.ParameterStatus] reflecting the new value.
     *
     * [searchPath] is sent as `search_path`, with schemas joined by commas.
     * [statementTimeout], [lockTimeout], and [idleInTransactionSessionTimeout] are sent
     * in milliseconds as required by the PostgreSQL GUC interface.
     */
    data class StartupMessage(
        val user:                            String,
        val database:                        String,
        val applicationName:                 String               = "minamoto",
        val searchPath:                      List<String>         = emptyList(),
        val timezone:                        String?              = null,
        val statementTimeout:                kotlin.time.Duration? = null,
        val lockTimeout:                     kotlin.time.Duration? = null,
        val idleInTransactionSessionTimeout: kotlin.time.Duration? = null,
    ) : FrontendMessage

    /** MD5 password response to [BackendMessage.AuthenticationMD5Password]. */
    data class PasswordMessage(val password: String) : FrontendMessage

    /**
     * Parses a SQL query into a named prepared statement.
     * Use an empty [statementName] for the unnamed statement.
     */
    data class Parse(
        val statementName: String,
        val statement:           String,
        val parameterOids: List<Int> = emptyList(),
    ) : FrontendMessage

    /**
     * Binds parameters to a prepared statement, creating a portal.
     * Use an empty [portalName] for the unnamed portal.
     */
    data class Bind(
        val portalName:    String,
        val statementName: String,
        val parameters:    Parameters,
        val resultFormats: List<Short> = emptyList(),
    ) : FrontendMessage

    /** Requests a description of a prepared statement or portal. */
    data class Describe(
        val target: DescribeTarget,
        val name:   String,
    ) : FrontendMessage

    /**
     * Executes a portal, fetching at most [maxRows] rows.
     * A [maxRows] of 0 means fetch all rows.
     *
     * Setting [maxRows] to the downstream demand count provides end-to-end backpressure —
     * the server only sends as many rows as the subscriber has requested.
     */
    data class Execute(
        val portalName: String,
        val maxRows:    Int = 0,
    ) : FrontendMessage

    /** Flushes the output buffer without closing the current portal, keeping the connection busy. */
    data object Flush : FrontendMessage

    /** Flushes the output buffer and waits for the server to respond. */
    data object Sync : FrontendMessage

    /** Closes a prepared statement or portal, freeing server-side resources. */
    data class Close(
        val target: DescribeTarget,
        val name:   String,
    ) : FrontendMessage

    /** Requests cancellation of the current operation. Sent on a new connection. */
    data class CancelRequest(
        val processId: Int,
        val secretKey: Int,
    ) : FrontendMessage

    /** Requests TLS upgrade. Server responds with 'S' (supported) or 'N' (not supported). */
    data object SSLRequest : FrontendMessage

    /** Terminates the connection gracefully. */
    data object Terminate : FrontendMessage

    /** SCRAM initial response — mechanism name + client-first message. */
    data class SASLInitialResponse(
        val mechanism:          String,
        val clientFirstMessage: ByteArray,
    ) : FrontendMessage

    /** SCRAM client-final message. */
    data class SASLResponse(val data: ByteArray) : FrontendMessage
}

internal sealed interface DescribeTarget {
    data object Statement : DescribeTarget
    data object Portal    : DescribeTarget
}

/**
 * Messages sent from the server to the client (backend messages).
 *
 * Each type maps directly to a PGwire backend message type.
 */
internal sealed interface BackendMessage {

    data class ParameterStatus(val name: String, val value: String) : BackendMessage

    data class KeyData(val processId: Int, val secretKey: Int) : BackendMessage

    /**
     * Server is ready to accept a new query.
     *
     * [transactionStatus] reflects the current transaction state:
     * 'I' = idle, 'T' = in transaction, 'E' = in failed transaction.
     */
    data class ReadyForQuery(val transactionStatus: TransactionStatus) : BackendMessage

    data class ParameterDescription(val parameterOids: List<Int>) : BackendMessage

    data class RowDescription(val columns: List<Column.Description>) : BackendMessage

    data class DataRow(val values: List<ByteArray?>) : BackendMessage

    data class CommandComplete(val tag: String) : BackendMessage

    data object ParseComplete : BackendMessage

    data object BindComplete : BackendMessage

    data object CloseComplete : BackendMessage

    data object PortalSuspended : BackendMessage

    data object NoData : BackendMessage

    data object EmptyQueryResponse : BackendMessage

    /**
     * [severity] — ERROR, FATAL, or PANIC.
     * [sqlState] — 5-character SQLSTATE code.
     * [detail] and [hint] carry the optional extended fields from the server error response.
     */
    data class ErrorResponse(
        val severity: String,
        val sqlState: String,
        val message:  String,
        val detail:   String = "",
        val hint:     String = "",
    ) : BackendMessage

    data class NoticeResponse(
        val severity: String,
        val sqlState: String,
        val message:  String,
        val detail:   String = "",
        val hint:     String = "",
    ) : BackendMessage

    data class NotificationResponse(
        val processId: Int,
        val channel:   String,
        val payload:   String,
    ) : BackendMessage
}

internal sealed interface TransactionStatus {
    data object Idle              : TransactionStatus
    data object InTransaction     : TransactionStatus
    /** A transaction is in a failed state — must be rolled back before further use. */
    data object FailedTransaction : TransactionStatus
}

/**
 * Authentication result from the server. Subclasses represent each authentication
 * mechanism the server may request or confirm.
 */
internal sealed class Authentication : BackendMessage {
    data object Ok                                  : Authentication()
    data object CleartextPassword                  : Authentication()
    data class  MD5Password(val salt: ByteArray)   : Authentication()
    data class  SASL(val mechanisms: List<String>) : Authentication()
    data class  SASLContinue(val data: ByteArray)  : Authentication()
    data class  SASLFinal(val data: ByteArray)     : Authentication()
}
