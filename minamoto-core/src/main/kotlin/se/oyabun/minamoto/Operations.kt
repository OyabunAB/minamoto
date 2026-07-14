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

import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import kotlin.reflect.KClass

/**
 * A single result row from a query.
 *
 * Use [get] when you expect a non-null value — it throws [MinamotoException.UnexpectedNull]
 * if the column value is null. Use [getOrNull] when nulls are legitimate.
 *
 * Type conversion is handled by the driver's codec registry. Requesting an unsupported
 * type throws [MinamotoException.CodecFailed].
 */
interface Row {
    fun <T : Any> get(column: String, type: KClass<T>): T
    fun <T : Any> getOrNull(column: String, type: KClass<T>): T?
    val metadata: RowMetadata
}

/**
 * Metadata for a single column in a result set.
 *
 * [type] reflects the database-side type. For PostgreSQL this is an OID ([ColumnType.Native]);
 * for sources without a native type system it may be [ColumnType.Named].
 */
data class ColumnMetadata(
    val name:     String,
    val index:    Int,
    val type:     ColumnType,
    val nullable: Nullability,
)

/**
 * Whether the database considers this column nullable.
 *
 * [Unknown] is returned when the driver cannot determine nullability from the wire protocol,
 * e.g. for computed columns or certain query shapes.
 */
sealed interface Nullability {
    data object Nullable    : Nullability
    data object NonNullable : Nullability
    data object Unknown     : Nullability
}

/**
 * The database-side type of a column.
 *
 * [Native] carries the raw OID as returned by PostgreSQL.
 * [Named] is used by drivers that identify types by name rather than numeric ID.
 */
sealed interface ColumnType {
    data class Named(val name: String) : ColumnType
    data class Native(val oid: Int)    : ColumnType
}

/**
 * Metadata for all columns in a result set.
 *
 * [column] looks up by name and throws [MinamotoException.UnknownColumn] if not found.
 * For positional access use [columns] directly.
 */
data class RowMetadata(val columns: List<ColumnMetadata>) {
    fun column(name: String): ColumnMetadata =
        columns.firstOrNull { it.name == name } ?: throw MinamotoException.UnknownColumn(name)
}

/**
 * A pre-configured, reusable query that maps result rows to [T].
 *
 * Define it once against a [Database], then call it with parameters as needed.
 * The underlying prepared statement is created on first execution and reused for
 * the lifetime of the connection.
 *
 * Example:
 * ```kotlin
 * val findUser = db.query("SELECT * FROM users WHERE id = $1") { row ->
 *     User(row.get("id"), row.get("name"))
 * }
 * val user: Many<User> = findUser(42)
 * ```
 */
fun interface Query<T : Any> {
    operator fun invoke(vararg params: Any): Many<T>
}

/**
 * A pre-configured, reusable command (INSERT / UPDATE / DELETE / DDL).
 *
 * Returns the number of rows affected. For DDL statements that do not affect rows,
 * the value is `0`.
 *
 * Example:
 * ```kotlin
 * val updateBalance = db.command("UPDATE accounts SET balance = $1 WHERE id = $2")
 * val affected: One<Long> = updateBalance(100, 42)
 * ```
 */
fun interface Command {
    operator fun invoke(vararg params: Any): One<Long>
}

/**
 * A pre-configured, reusable command with no meaningful return value.
 *
 * Use for fire-and-forget operations like `NOTIFY`, `SET`, or DDL where
 * the row count is irrelevant.
 */
fun interface Effect {
    operator fun invoke(vararg params: Any): None<Unit>
}

/**
 * A pre-configured, reusable batch command.
 *
 * Sends multiple parameter sets in a single round-trip. Each element of [batches]
 * is one set of bind parameters. Returns affected row counts in the same order
 * as the input batches.
 *
 * Example:
 * ```kotlin
 * val insertUser = db.batch("INSERT INTO users (id, name) VALUES ($1, $2)")
 * val counts: Many<Long> = insertUser(listOf(arrayOf(1, "walter"), arrayOf(2, "jesse")))
 * ```
 */
fun interface Batch {
    operator fun invoke(batches: List<Array<out Any>>): Many<Long>
}

/**
 * The top-level handle for interacting with the database.
 *
 * Operations are defined once via [query], [command], [effect], or [batch] and called
 * repeatedly with parameters. Connection acquisition and release is fully transparent —
 * the caller never holds a connection directly.
 *
 * Transactions are scoped to a coroutine via [transaction]. Any operation executed within
 * a [transaction] block participates automatically — no connection or transaction handle
 * is passed around.
 *
 * Example:
 * ```kotlin
 * db.transaction {
 *     debit(accountId, amount)
 *     credit(targetId, amount)
 * }
 * ```
 *
 * Nested [transaction] calls with [TransactionMode.Join] reuse the active transaction.
 * [TransactionMode.New] always starts a fresh transaction, becoming a savepoint if one
 * is already active.
 */
interface Database {

    fun <T : Any> query(sql: String, map: (Row) -> T): Query<T>
    fun command(sql: String): Command
    fun effect(sql: String): Effect
    fun batch(sql: String): Batch

    suspend fun <T> transaction(
        mode:       TransactionMode       = TransactionMode.Join,
        definition: TransactionDefinition = TransactionDefinition(),
        block:      suspend () -> T,
    ): T
}
