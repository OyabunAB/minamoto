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
import se.oyabun.minamoto.MinamotoException

/**
 * Handles one-dimensional Postgres arrays in binary format.
 *
 * Wire layout: int32 ndim, int32 hasNulls flag, int32 elementOid, then per-dimension
 * int32 length + int32 lbound, then per-element int32 length (-1 = null) + bytes.
 *
 * Null elements are not supported — [se.oyabun.minamoto.MinamotoException.CodecFailed]
 * is thrown if the server sends one. Multi-dimensional arrays are rejected by checking ndim.
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
        buffer.putInt(1)
        buffer.putInt(0)
        buffer.putInt(elementOid)
        buffer.putInt(value.size)
        buffer.putInt(1)
        for (elementBytes in elementBuffers) {
            buffer.putInt(elementBytes.size)
            buffer.put(elementBytes)
        }
        return Pair(buffer.array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): List<T> {
        val buffer = ByteBuffer.wrap(bytes)
        val ndim   = buffer.int
        buffer.int
        buffer.int

        if (ndim == 0) return emptyList()

        val size = buffer.int
        buffer.int

        val result = ArrayList<T>(size)
        repeat(size) { index ->
            val length = buffer.int
            if (length == -1) throw MinamotoException.CodecFailed(
                "null element at index $index in array OID $sourceOid — use getOrNull or handle nulls before storing in arrays"
            )
            val elementBytes = ByteArray(length)
            buffer.get(elementBytes)
            result.add(elementCodec.decode(elementBytes, elementOid))
        }
        return result
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
)
