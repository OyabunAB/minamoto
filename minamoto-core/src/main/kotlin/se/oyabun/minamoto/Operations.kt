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
import se.oyabun.aelv.Maybe
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

/** A named parameter binding — name to value. Use Kotlin's `to` infix: `"id" to 42`. */
typealias Binding = Pair<String, Any>

/**
 * A query ready to have parameters bound.
 *
 * Call [bind] with named parameter bindings to produce a [BoundQuery], or call the
 * terminal methods directly when the query has no parameters.
 */
interface QueryBuilder {
    fun bind(vararg bindings: Binding): BoundQuery
    fun multiple(): Many<Row>  = bind().multiple()
    fun single():   One<Row>   = bind().single()
    fun optional(): Maybe<Row> = bind().optional()
}

/**
 * A query with parameters encoded, ready to stream rows.
 *
 * [multiple] streams all result rows. [single] returns the first row and errors if zero rows are
 * returned — it does NOT error on more than one row; extra rows are discarded.
 * [optional] returns the first row or an empty [Maybe].
 */
interface BoundQuery {
    fun multiple(): Many<Row>
    fun single():   One<Row>
    fun optional(): Maybe<Row>
}

/**
 * A command (INSERT / UPDATE / DELETE / DDL) ready to have parameters bound.
 *
 * Call [bind] with named parameter bindings to produce a [BoundCommand], or call
 * [execute] directly when the command has no parameters.
 */
interface CommandBuilder {
    fun bind(vararg bindings: Binding): BoundCommand
    fun count(): One<Long> = bind().count()
}

/**
 * A command with parameters encoded, ready to execute.
 *
 * Returns the number of rows affected. DDL returns 0.
 */
interface BoundCommand {
    fun count(): One<Long>
}

/**
 * A fire-and-forget statement ready to have parameters bound.
 *
 * Use for DDL, NOTIFY, SET, and any statement where the result is irrelevant.
 * Call [bind] with named parameter bindings to produce a [BoundEffect], or call
 * [execute] directly when the statement has no parameters.
 */
interface EffectBuilder {
    fun bind(vararg bindings: Binding): BoundEffect
    fun execute(): None<Unit> = bind().execute()
}

/** A fire-and-forget statement with parameters encoded, ready to execute. */
interface BoundEffect {
    fun execute(): None<Unit>
}

/**
 * The top-level handle for interacting with the database.
 *
 * Named parameters in queries use `:name` syntax — bind them with Kotlin's `to` infix:
 * `"id" to 42`.
 *
 * Connection acquisition and release is fully transparent — callers never hold a
 * connection directly.
 *
 * Transactions are scoped to a coroutine via [transaction]. Any operation executed
 * within a [transaction] block participates automatically.
 *
 * Nested [transaction] calls with [TransactionMode.Join] reuse the active transaction.
 * [TransactionMode.New] always starts a fresh transaction, becoming a savepoint if one
 * is already active.
 */
interface Database {
    fun query(statement: String):   QueryBuilder
    fun command(statement: String): CommandBuilder
    fun effect(statement: String):  EffectBuilder

    /** If [block] throws, the transaction is automatically rolled back before the exception propagates to the caller. */
    suspend fun <T> transaction(
        mode:       TransactionMode       = TransactionMode.Join,
        definition: TransactionDefinition = TransactionDefinition(),
        block:      suspend () -> T,
    ): T
}
