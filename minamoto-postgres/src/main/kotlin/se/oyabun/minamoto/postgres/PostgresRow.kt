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

import se.oyabun.minamoto.ColumnMetadata
import se.oyabun.minamoto.ColumnType
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.Nullability
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.RowMetadata
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import kotlin.reflect.KClass

/**
 * A single column in a result row — description and value together.
 *
 * Keeping them as one type eliminates any possibility of index mismatch
 * between a parallel description list and a parallel value list.
 */
internal data class Column(
    val description: Description,
    val value:       Value,
) {
    /**
     * Wire-level metadata for this column, as reported by the server's RowDescription message.
     *
     * [formatCode] reflects what format the server used when sending this column's bytes —
     * 0 = text, 1 = binary. The codec uses this to decode correctly.
     */
    data class Description(
        val name:         String,
        val tableOid:     Int,
        val columnIndex:  Short,
        val typeOid:      Int,
        val typeSize:     Short,
        val typeModifier: Int,
        val formatCode:   Short,
    )

    /**
     * The value of this column as received from the wire, before codec decoding.
     *
     * [Missing] represents SQL NULL — the server sent -1 for the column length.
     * [Present] carries the raw bytes as received, ready for codec decoding.
     */
    sealed interface Value {
        data class  Present(val bytes: ByteArray) : Value
        data object Missing                       : Value
    }
}

/**
 * [Row] implementation backed by [Column]s from the wire and a [CodecRegistry].
 *
 * Column lookup is by name, case-insensitive. [get] throws [MinamotoException.UnexpectedNull]
 * when the value is [Column.Value.Missing]. [getOrNull] returns null in that case.
 *
 * Codec dispatch uses the column's OID from [Column.Description].
 * The registry handles numeric widening — requesting [Long] for an [Int] column succeeds
 * without explicit registration.
 */
internal class PostgresRow(
    private val columns:  List<Column>,
    private val registry: CodecRegistry,
) : Row {

    override val metadata: RowMetadata by lazy {
        RowMetadata(columns.mapIndexed { index, column ->
            ColumnMetadata(
                name     = column.description.name,
                index    = index,
                type     = ColumnType.Native(column.description.typeOid),
                nullable = Nullability.Unknown,
            )
        })
    }

    override fun <T : Any> get(column: String, type: KClass<T>): T {
        val found = find(column)
        return when (found.value) {
            is Column.Value.Missing -> throw MinamotoException.UnexpectedNull(column)
            is Column.Value.Present -> registry.find(found.description.typeOid, type)
                                               .decode(found.value.bytes, found.description.typeOid)
        }
    }

    override fun <T : Any> getOrNull(column: String, type: KClass<T>): T? {
        val found = find(column)
        return when (found.value) {
            is Column.Value.Missing -> null
            is Column.Value.Present -> registry.find(found.description.typeOid, type)
                                               .decode(found.value.bytes, found.description.typeOid)
        }
    }

    private fun find(name: String): Column =
        columns.firstOrNull { it.description.name.equals(name, ignoreCase = true) }
            ?: throw MinamotoException.UnknownColumn(name)
}

/** Reified extension — captures [T] at the call site and dispatches to [Row.get]. */
inline fun <reified T : Any> Row.get(column: String): T = get(column, T::class)

/** Reified extension — captures [T] at the call site and dispatches to [Row.getOrNull]. */
inline fun <reified T : Any> Row.getOrNull(column: String): T? = getOrNull(column, T::class)
