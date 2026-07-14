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

/**
 * Messages sent from the client to the server (frontend messages).
 *
 * Each type maps directly to a PGwire frontend message type.
 * See: https://www.postgresql.org/docs/current/protocol-message-formats.html
 */
sealed interface FrontendMessage {

    /** Initiates a connection. Sent once before authentication. */
    data class StartupMessage(
        val user:            String,
        val database:        String,
        val applicationName: String = "minamoto",
    ) : FrontendMessage

    /** MD5 password response to [BackendMessage.AuthenticationMD5Password]. */
    data class PasswordMessage(val password: String) : FrontendMessage

    /**
     * Parses a SQL query into a named prepared statement.
     * Use an empty [statementName] for the unnamed statement.
     */
    data class Parse(
        val statementName: String,
        val sql:           String,
        val parameterOids: List<Int> = emptyList(),
    ) : FrontendMessage

    /**
     * Binds parameters to a prepared statement, creating a portal.
     * Use an empty [portalName] for the unnamed portal.
     */
    data class Bind(
        val portalName:    String,
        val statementName: String,
        val parameters:    List<ByteArray?>,
        val resultFormats: List<Short> = emptyList(),
    ) : FrontendMessage

    /**
     * Requests a description of a prepared statement or portal.
     */
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

    /** Terminates the connection gracefully. */
    data object Terminate : FrontendMessage

    /** SCRAM initial response — mechanism name + client-first message. */
    data class SASLInitialResponse(
        val mechanism:        String,
        val clientFirstMessage: ByteArray,
    ) : FrontendMessage

    /** SCRAM client-final message. */
    data class SASLResponse(val data: ByteArray) : FrontendMessage
}

sealed interface DescribeTarget {
    data object Statement : DescribeTarget
    data object Portal    : DescribeTarget
}

/**
 * Messages sent from the server to the client (backend messages).
 *
 * Each type maps directly to a PGwire backend message type.
 */
sealed interface BackendMessage {

    /** Authentication succeeded — connection is ready to use. */
    data object AuthenticationOk : BackendMessage

    /** Server requests SCRAM authentication. [mechanisms] lists supported SCRAM variants. */
    data class AuthenticationSASL(val mechanisms: List<String>) : BackendMessage

    /** Server sends the SCRAM server-first message. */
    data class AuthenticationSASLContinue(val data: ByteArray) : BackendMessage

    /** Server sends the SCRAM server-final message for verification. */
    data class AuthenticationSASLFinal(val data: ByteArray) : BackendMessage

    /** Server requests a cleartext password. */
    data object AuthenticationCleartextPassword : BackendMessage

    /**
     * Server requests an MD5-hashed password.
     * Hash as: md5(md5(password + username) + salt), prefixed with "md5".
     */
    data class AuthenticationMD5Password(val salt: ByteArray) : BackendMessage

    /** Server reports a runtime parameter value (e.g. server_version, TimeZone). */
    data class ParameterStatus(val name: String, val value: String) : BackendMessage

    /** Server reports its process ID and secret key, used for cancel requests. */
    data class BackendKeyData(val processId: Int, val secretKey: Int) : BackendMessage

    /**
     * Server is ready to accept a new query.
     *
     * [transactionStatus] reflects the current transaction state:
     * 'I' = idle, 'T' = in transaction, 'E' = in failed transaction.
     */
    data class ReadyForQuery(val transactionStatus: TransactionStatus) : BackendMessage

    /** Describes the parameters of a prepared statement. */
    data class ParameterDescription(val parameterOids: List<Int>) : BackendMessage

    /** Describes the columns of a query result. */
    data class RowDescription(val columns: List<ColumnDescription>) : BackendMessage

    /** A single data row from a query result. Each element is a raw column value or null. */
    data class DataRow(val values: List<ByteArray?>) : BackendMessage

    /** Query completed successfully. [tag] is the command tag (e.g. "INSERT 0 1"). */
    data class CommandComplete(val tag: String) : BackendMessage

    /** Parse completed successfully. */
    data object ParseComplete : BackendMessage

    /** Bind completed successfully. */
    data object BindComplete : BackendMessage

    /** Close completed successfully. */
    data object CloseComplete : BackendMessage

    /** Portal suspended — more rows are available, request more via Execute. */
    data object PortalSuspended : BackendMessage

    /** No data to return (e.g. Describe of a statement that returns no rows). */
    data object NoData : BackendMessage

    /** Empty query string received. */
    data object EmptyQueryResponse : BackendMessage

    /**
     * Server reported an error.
     *
     * [severity] — ERROR, FATAL, or PANIC.
     * [sqlState] — 5-character SQLSTATE code.
     * [message]  — primary human-readable message.
     * [detail]   — optional additional detail.
     * [hint]     — optional hint for the user.
     */
    data class ErrorResponse(
        val severity: String,
        val sqlState: String,
        val message:  String,
        val detail:   String = "",
        val hint:     String = "",
    ) : BackendMessage

    /** Server sent a non-fatal notice. Same fields as [ErrorResponse]. */
    data class NoticeResponse(
        val severity: String,
        val sqlState: String,
        val message:  String,
        val detail:   String = "",
        val hint:     String = "",
    ) : BackendMessage

    /** Async notification from LISTEN/NOTIFY. */
    data class NotificationResponse(
        val processId: Int,
        val channel:   String,
        val payload:   String,
    ) : BackendMessage
}

/**
 * The transaction state reported by the server in [BackendMessage.ReadyForQuery].
 */
sealed interface TransactionStatus {
    /** No transaction is active. */
    data object Idle              : TransactionStatus
    /** A transaction is in progress. */
    data object InTransaction     : TransactionStatus
    /** A transaction is in a failed state — must be rolled back. */
    data object FailedTransaction : TransactionStatus
}

/**
 * Metadata for a single column in a result set, as reported by [BackendMessage.RowDescription].
 */
data class ColumnDescription(
    val name:         String,
    val tableOid:     Int,
    val columnIndex:  Short,
    val typeOid:      Int,
    val typeSize:     Short,
    val typeModifier: Int,
    val formatCode:   Short,
)
