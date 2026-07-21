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
 * Codec for the Postgres `hstore` extension type, represented as `Map<String, String?>`.
 *
 * **Binary wire format**:
 * ```
 * int32  count               — number of key-value pairs
 * for each pair:
 *   int32  key_len           — byte length of the key
 *   bytes  key               — UTF-8 key
 *   int32  val_len           — byte length of the value, or -1 for SQL NULL
 *   bytes  val               — UTF-8 value (absent when val_len == -1)
 * ```
 *
 * The Postgres OID is assigned when the `hstore` extension is installed and
 * therefore differs per database. The OID is resolved lazily on the first
 * decode — register this codec before creating the pool via
 * [CodecRegistry.registerHstore].
 *
 * **First-query text fallback**: because [CodecRegistry.preferredFormat] cannot
 * locate this codec before the OID is lazily bound, the first query receives a
 * TEXT-format response (`"key"=>"value",...`). The decoder detects text by the
 * absence of a leading int32 count that would be valid as binary and falls back
 * to a text parser. Subsequent queries negotiate binary via the cached OID entry.
 *
 * Requires `CREATE EXTENSION hstore` in the target database.
 */
object HstoreCodec : Codec<Map<String, String?>> {

    override val oid             = 0    // resolved lazily from RowDescription
    override val type            = Map::class as kotlin.reflect.KClass<Map<String, String?>>
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Map<String, String?>): Pair<ByteArray, FormatCode> {
        val pairs     = value.entries.toList()
        val keyBytes  = pairs.map { it.key.toByteArray(Charsets.UTF_8) }
        val valBytes  = pairs.map { it.value?.toByteArray(Charsets.UTF_8) }
        val size      = 4 + pairs.indices.sumOf { i ->
            4 + keyBytes[i].size + 4 + (valBytes[i]?.size ?: 0)
        }
        val buf = ByteBuffer.allocate(size)
        buf.putInt(pairs.size)
        pairs.indices.forEach { i ->
            buf.putInt(keyBytes[i].size)
            buf.put(keyBytes[i])
            val vb = valBytes[i]
            if (vb == null) buf.putInt(-1) else { buf.putInt(vb.size); buf.put(vb) }
        }
        return buf.array() to FormatCode.BINARY
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): Map<String, String?> =
        if (looksLikeText(bytes)) decodeText(bytes) else decodeBinary(bytes)

    // -------------------------------------------------------------------------

    /**
     * Heuristic: if the first 4 bytes interpreted as an int32 count would exceed
     * the remaining byte count (impossible for valid binary), treat as text.
     * An empty hstore has count = 0 in binary; empty text is the empty string "".
     */
    private fun looksLikeText(bytes: ByteArray): Boolean {
        if (bytes.isEmpty()) return false
        if (bytes[0] == '"'.code.toByte() || bytes[0] == ' '.code.toByte()) return true
        if (bytes.size < 4) return true
        val count = ByteBuffer.wrap(bytes, 0, 4).int
        return count < 0 || count.toLong() * 8 > bytes.size  // min 8 bytes per pair
    }

    private fun decodeBinary(bytes: ByteArray): Map<String, String?> {
        val buf   = ByteBuffer.wrap(bytes)
        val count = buf.int
        return buildMap(count) {
            repeat(count) {
                val keyLen = buf.int
                val key    = String(ByteArray(keyLen).also { buf.get(it) }, Charsets.UTF_8)
                val valLen = buf.int
                val value  = if (valLen == -1) null
                             else String(ByteArray(valLen).also { buf.get(it) }, Charsets.UTF_8)
                put(key, value)
            }
        }
    }

    /**
     * Parses the hstore text format: `"key1"=>"val1","key2"=>NULL,...`
     *
     * Keys are always double-quoted. Values are either double-quoted strings or the
     * unquoted token `NULL`. Inside double quotes, `\"` and `\\` are escape sequences.
     */
    private fun decodeText(bytes: ByteArray): Map<String, String?> {
        val text = bytes.toString(Charsets.UTF_8).trim()
        if (text.isEmpty()) return emptyMap()
        val result = mutableMapOf<String, String?>()
        var pos    = 0

        fun skipSpaces() { while (pos < text.length && text[pos] == ' ') pos++ }

        fun readQuoted(): String {
            check(text[pos] == '"') { "expected '\"' at pos $pos in hstore text: $text" }
            pos++ // skip opening quote
            val sb = StringBuilder()
            while (pos < text.length && text[pos] != '"') {
                if (text[pos] == '\\' && pos + 1 < text.length) {
                    pos++; sb.append(text[pos])
                } else {
                    sb.append(text[pos])
                }
                pos++
            }
            check(pos < text.length) { "unterminated quoted string in hstore text: $text" }
            pos++ // skip closing quote
            return sb.toString()
        }

        while (pos < text.length) {
            skipSpaces()
            if (pos >= text.length) break
            val key = readQuoted()
            skipSpaces()
            check(pos + 1 < text.length && text[pos] == '=' && text[pos + 1] == '>') {
                "expected '=>' at pos $pos in hstore text: $text"
            }
            pos += 2
            skipSpaces()
            val value = if (pos < text.length && text[pos] == '"') readQuoted()
                        else {
                            val start = pos
                            while (pos < text.length && text[pos] != ',' && text[pos] != ' ') pos++
                            val token = text.substring(start, pos)
                            if (token.equals("NULL", ignoreCase = true)) null else token
                        }
            result[key] = value
            skipSpaces()
            if (pos < text.length && text[pos] == ',') pos++
        }

        return result
    }
}
