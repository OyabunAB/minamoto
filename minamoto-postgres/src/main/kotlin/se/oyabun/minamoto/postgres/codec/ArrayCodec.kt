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
package se.oyabun.minamoto.postgres.codec

import java.nio.ByteBuffer
import kotlin.reflect.KClass
import se.oyabun.minamoto.DatabaseException

/**
 * Handles Postgres arrays of any number of dimensions in binary format.
 *
 * **Wire layout**: `int32 ndim`, `int32 hasNulls`, `int32 elementOid`, then per-dimension
 * `int32 length + int32 lbound`, then all elements in **row-major order** as
 * `int32 length` (-1 = null) + bytes.
 *
 * All elements are read into a flat list, then [reshape] partitions them into
 * nested [List]s matching the declared dimension sizes:
 * - `ndim = 1` → `List<T>`
 * - `ndim = 2` → `List<List<T>>`
 * - `ndim = N` → N levels of nested lists
 *
 * Null elements throw [DatabaseException.CodecFailed].
 * [encode] always produces a 1-D array; supply a flat [List] as the value.
 */
internal class ArrayCodec<T : Any>(
    override val oid:          Int,
    private  val elementOid:   Int,
    private  val elementCodec: Codec<T>,
) : Codec<List<T>> {

    @Suppress("UNCHECKED_CAST")
    override val type: KClass<List<T>> = List::class as KClass<List<T>>

    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: List<T>): Pair<ByteArray, FormatCode> {
        val elementBuffers = value.map { elementCodec.encode(it).first }
        val buffer         = ByteBuffer.allocate(20 + elementBuffers.sumOf { 4 + it.size })
        buffer.putInt(1)           // ndim
        buffer.putInt(0)           // hasNulls
        buffer.putInt(elementOid)
        buffer.putInt(value.size)  // dim[0] length
        buffer.putInt(1)           // dim[0] lbound
        for (elementBytes in elementBuffers) {
            buffer.putInt(elementBytes.size)
            buffer.put(elementBytes)
        }
        return Pair(buffer.array(), FormatCode.BINARY)
    }

    @Suppress("UNCHECKED_CAST")
    override fun decode(bytes: ByteArray, sourceOid: Int): List<T> {
        val buffer = ByteBuffer.wrap(bytes)
        val ndim   = buffer.int
        buffer.int  // hasNulls (ignored)
        buffer.int  // elementOid from wire (we already have it)

        if (ndim == 0) return emptyList()

        // One (length, lbound) pair per dimension; lower bound is always 1 in PG.
        val dimensions = IntArray(ndim)
        repeat(ndim) { d ->
            dimensions[d] = buffer.int  // dimension size
            buffer.int                   // discard lower bound
        }

        // Elements arrive flat in row-major order regardless of ndim.
        val total = dimensions.fold(1, Int::times)
        val flat  = ArrayList<T>(total)
        repeat(total) { index ->
            val length = buffer.int
            if (length == -1) throw DatabaseException.CodecFailed(
                "null element at flat index $index in $ndim-D array OID $sourceOid"
            )
            val elementBytes = ByteArray(length)
            buffer.get(elementBytes)
            flat.add(elementCodec.decode(elementBytes, elementOid))
        }

        return reshape(flat as List<Any?>, dimensions, 0) as List<T>
    }

    /**
     * Recursively partitions a flat element list into nested lists that match the
     * declared dimension sizes.
     *
     * At depth [depth] the inner count is the product of all dimension sizes below
     * [depth] — that is the size of each chunk that belongs to one outer-dimension
     * slot. Each chunk is then recursively reshaped at [depth]+1 until the last
      * dimension is reached, where the slice is returned as-is.
      */
    private fun reshape(elements: List<Any?>, dimensions: IntArray, depth: Int): List<*> =
        if (depth == dimensions.lastIndex) ArrayList(elements)  // copy — windowed() reuses its view object
        else {
            val innerCount = (depth + 1..dimensions.lastIndex).fold(1) { acc, i -> acc * dimensions[i] }
            elements.chunked(innerCount) { slice -> reshape(slice, dimensions, depth + 1) }
        }
}

internal val arrayOidByElementOid: Map<Int, Int> = mapOf(
    Oid.BOOL        to Oid.BOOL_ARRAY,
    Oid.INT2        to Oid.INT2_ARRAY,
    Oid.INT4        to Oid.INT4_ARRAY,
    Oid.INT8        to Oid.INT8_ARRAY,
    Oid.FLOAT4      to Oid.FLOAT4_ARRAY,
    Oid.FLOAT8      to Oid.FLOAT8_ARRAY,
    Oid.TEXT        to Oid.TEXT_ARRAY,
    Oid.VARCHAR     to Oid.VARCHAR_ARRAY,
    Oid.BPCHAR      to Oid.BPCHAR_ARRAY,
    Oid.DATE        to Oid.DATE_ARRAY,
    Oid.TIME        to Oid.TIME_ARRAY,
    Oid.TIMESTAMP   to Oid.TIMESTAMP_ARRAY,
    Oid.TIMESTAMPTZ to Oid.TIMESTAMPTZ_ARRAY,
    Oid.TIMETZ      to Oid.TIMETZ_ARRAY,
    Oid.INTERVAL    to Oid.INTERVAL_ARRAY,
    Oid.UUID        to Oid.UUID_ARRAY,
    Oid.NUMERIC     to Oid.NUMERIC_ARRAY,
    Oid.BYTEA       to Oid.BYTEA_ARRAY,
    Oid.JSON        to Oid.JSON_ARRAY,
    Oid.JSONB       to Oid.JSONB_ARRAY,
    Oid.INET        to Oid.INET_ARRAY,
    Oid.POINT       to Oid.POINT_ARRAY,
    Oid.LSEG        to Oid.LSEG_ARRAY,
    Oid.PATH        to Oid.PATH_ARRAY,
    Oid.BOX         to Oid.BOX_ARRAY,
    Oid.POLYGON     to Oid.POLYGON_ARRAY,
    Oid.LINE        to Oid.LINE_ARRAY,
    Oid.CIRCLE      to Oid.CIRCLE_ARRAY,
)
