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

/**
 * Codec for the pgvector `vector` type, represented as [FloatArray].
 *
 * Binary wire format (pgvector ≥ 0.5):
 * ```
 * int16   dim      — number of dimensions
 * int16   unused   — always 0; reserved for future use
 * float32 x[0]
 * float32 x[1]
 * ...
 * float32 x[dim-1]
 * ```
 *
 * The Postgres OID is assigned when the `vector` extension is installed and
 * therefore differs per database. The OID is resolved lazily on the first
 * decode — register this codec before creating the pool via
 * [CodecRegistry.registerVector].
 *
 * **First-query text fallback**: because [CodecRegistry.preferredFormat] cannot
 * know this codec exists until after the first decode lazily binds the OID,
 * the first query receives a TEXT-format response (`[x,y,z,...]`). The decoder
 * detects text by the leading `[` and parses it; subsequent queries correctly
 * negotiate binary via the cached OID entry.
 *
 * Requires `CREATE EXTENSION vector` in the target database.
 */
object PgVectorCodec : Codec<FloatArray> {

    override val oid             = 0    // resolved lazily from RowDescription
    override val type            = FloatArray::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: FloatArray): Pair<ByteArray, FormatCode> {
        val buffer = ByteBuffer.allocate(4 + value.size * 4)
        buffer.putShort(value.size.toShort())
        buffer.putShort(0)
        value.forEach { buffer.putFloat(it) }
        return buffer.array() to FormatCode.BINARY
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): FloatArray =
        // The first query against a type-only codec receives TEXT format because
        // preferredFormat() cannot find the codec before the OID is lazily bound.
        // Detect text wire representation by the leading '[' and parse accordingly;
        // subsequent queries use binary after the OID is cached.
        if (bytes.isNotEmpty() && bytes[0] == '['.code.toByte()) decodeText(bytes)
        else                                                       decodeBinary(bytes)

    private fun decodeText(bytes: ByteArray): FloatArray =
        bytes.toString(Charsets.UTF_8)
            .trim('[', ']')
            .split(",")
            .map { it.trim().toFloat() }
            .toFloatArray()

    private fun decodeBinary(bytes: ByteArray): FloatArray {
        val buffer = ByteBuffer.wrap(bytes)
        val dim    = buffer.short.toInt() and 0xFFFF
        buffer.short  // skip unused
        return FloatArray(dim) { buffer.float }
    }
}
