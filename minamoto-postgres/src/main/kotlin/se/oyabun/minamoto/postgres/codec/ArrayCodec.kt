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
 * All elements are read into a flat list, then [reshapeFlat] partitions them into
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

        return reshapeFlat(flat as List<Any?>, dimensions, 0) as List<T>
    }
}

/**
 * Handles Postgres arrays of user-defined types (enums, domains, composite types)
 * whose element OID is not known until the first [decode] call.
 *
 * The [elementOid] is read from the binary array header at decode time and used to
 * look up the element codec in [registry] via [CodecRegistry.find]. This lazy lookup
 * also lazily binds the element codec under the real OID for O(1) future access.
 *
 * **First-query text fallback**: on the first query the array OID has not yet been
 * bound, so [CodecRegistry.preferredFormat] returns TEXT. The decoder detects the
 * leading `{` character and parses the PG text array representation instead.
 *
 * **Multiple user-defined array types**: if more than one element type is registered,
 * only the most recently registered codec occupies the type-only slot for `List`.
 * The first query for any given array OID binds the correct codec via the binary
 * header's `elementOid`; subsequent queries are O(1). The only risk is the first
 * query in TEXT format when two different array types are both unbound — in that
 * case the wrong element codec may be selected and will fail loudly with
 * [DatabaseException.CodecFailed] rather than silently producing wrong values.
 *
 * [encode] produces a PG text array `{label1,label2,...}` so that PG can infer
 * the target array type from the column/parameter context.
 */
internal class DynamicArrayCodec<T : Any>(
    private val elementType: KClass<T>,
    private val registry:    CodecRegistry,
) : Codec<List<T>> {

    override val oid             = 0    // resolved lazily
    @Suppress("UNCHECKED_CAST")
    override val type: KClass<List<T>> = List::class as KClass<List<T>>
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: List<T>): Pair<ByteArray, FormatCode> {
        if (value.isEmpty()) return "{}".toByteArray(Charsets.UTF_8) to FormatCode.TEXT
        val labels = value.joinToString(",") { element ->
            registry.findForEncoding(element).encode(element).first.toString(Charsets.UTF_8)
        }
        return "{$labels}".toByteArray(Charsets.UTF_8) to FormatCode.TEXT
    }

    @Suppress("UNCHECKED_CAST")
    override fun decode(bytes: ByteArray, sourceOid: Int): List<T> =
        if (bytes.isNotEmpty() && bytes[0] == '{'.code.toByte()) decodeText(bytes)
        else                                                       decodeBinary(bytes, sourceOid)

    private fun decodeText(bytes: ByteArray): List<T> {
        val text = bytes.toString(Charsets.UTF_8).trim()
        if (text == "{}" || text.isEmpty()) return emptyList()
        val content = text.removePrefix("{").removeSuffix("}")
        // Use OID 0 as a sentinel — type-only lookup resolves the element codec.
        val elementCodec = registry.find(0, elementType)
        return content.split(",").map { label ->
            elementCodec.decode(label.trim().toByteArray(Charsets.UTF_8), 0)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun decodeBinary(bytes: ByteArray, sourceOid: Int): List<T> {
        val buffer     = ByteBuffer.wrap(bytes)
        val ndim       = buffer.int
        buffer.int     // hasNulls (ignored)
        val elementOid = buffer.int

        if (ndim == 0) return emptyList()

        // Look up (and lazily bind) the element codec using the actual wire OID.
        val elementCodec = registry.find(elementOid, elementType)

        val dimensions = IntArray(ndim)
        repeat(ndim) { d ->
            dimensions[d] = buffer.int
            buffer.int   // discard lower bound
        }

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

        return reshapeFlat(flat as List<Any?>, dimensions, 0) as List<T>
    }
}

/**
 * Recursively partitions a flat element list into nested lists matching the
 * declared [dimensions].
 *
 * At depth [depth] the inner count is the product of all dimension sizes below
 * [depth]. Each chunk is recursively reshaped at [depth]+1 until the last
 * dimension is reached.
 *
 * **Important**: returns `ArrayList(elements)` at the base case — not `elements`
 * directly. Kotlin's `windowed()` reuses a single view object for `RandomAccess`
 * lists; returning the view would cause all outer slots to alias the same reference.
 */
internal fun reshapeFlat(elements: List<Any?>, dimensions: IntArray, depth: Int): List<*> =
    if (depth == dimensions.lastIndex) ArrayList(elements)
    else {
        val innerCount = (depth + 1..dimensions.lastIndex).fold(1) { acc, i -> acc * dimensions[i] }
        elements.chunked(innerCount) { slice -> reshapeFlat(slice, dimensions, depth + 1) }
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
