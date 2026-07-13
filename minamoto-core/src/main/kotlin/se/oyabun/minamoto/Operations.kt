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

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

/**
 * A single row returned from a query.
 *
 * Values are retrieved by column name using reified type inference —
 * no Java class literals.
 */
interface Row {
    fun <T : Any> get(column: String): T
    fun <T : Any> getOrNull(column: String): T?
    val metadata: RowMetadata
}

data class ColumnMetadata(
    val name:     String,
    val index:    Int,
    val type:     ColumnType,
    val nullable: Nullability,
)

sealed interface Nullability {
    data object Nullable    : Nullability
    data object NonNullable : Nullability
    data object Unknown     : Nullability
}

sealed interface ColumnType {
    data class Named(val name: String)     : ColumnType
    data class Native(val oid: Int)        : ColumnType
}

data class RowMetadata(val columns: List<ColumnMetadata>) {
    fun column(name: String): ColumnMetadata =
        columns.firstOrNull { it.name == name }
            ?: throw MinamotoException.UnknownColumn(name)
}

// ---------------------------------------------------------------------------
// Operations — defined once, called many times
// ---------------------------------------------------------------------------

/**
 * A pre-configured query that maps rows to [T].
 *
 * Call with parameters to produce a [Many] of results.
 * The underlying statement is prepared on first use and reused.
 */
fun interface Query<T : Any> {
    operator fun invoke(vararg params: Any): Many<T>
}

/**
 * A pre-configured command (INSERT / UPDATE / DELETE / DDL).
 *
 * Returns the number of rows affected.
 */
fun interface Command {
    operator fun invoke(vararg params: Any): One<Long>
}

/**
 * A pre-configured command with no meaningful return value.
 */
fun interface Effect {
    operator fun invoke(vararg params: Any): None<Unit>
}

/**
 * A pre-configured batch command.
 *
 * Each element in [batches] is one set of parameters.
 * Returns affected row counts in order.
 */
fun interface Batch {
    operator fun invoke(batches: List<Array<out Any>>): Many<Long>
}

// ---------------------------------------------------------------------------
// Database handle — the entry point
// ---------------------------------------------------------------------------

/**
 * The top-level handle for defining and executing database operations.
 *
 * Acquires connections from the pool transparently.
 * Participates in any [ConnectionContext] already on the coroutine context.
 */
interface Database {

    fun <T : Any> query(
        sql: String,
        map: (Row) -> T,
    ): Query<T>

    fun command(sql: String): Command

    fun effect(sql: String): Effect

    fun batch(sql: String): Batch

    suspend fun <T> transaction(
        mode:       TransactionMode        = TransactionMode.Join,
        definition: TransactionDefinition  = TransactionDefinition(),
        block:      suspend () -> T,
    ): T
}
