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

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import kotlin.reflect.KClass
import kotlin.reflect.KType

/**
 * Encodes and decodes a [T] directly to/from a json or jsonb column using kotlinx.serialization.
 *
 * [sourceOid] == [Oid.JSONB]: binary jsonb prepends a version byte (always 0x01) before
 * the JSON text. This codec strips it on decode and prepends it on encode.
 *
 * The [Json] instance is supplied by [PgCodecRegistry] and shared across all JSON codecs
 * registered for a given pool — configure it there to add custom serializers or modules.
 */
internal class JsonCodec<T : Any>(
    override val oid:        Int,
    override val type:       KClass<T>,
    private  val serializer: KSerializer<T>,
    private  val json:       Json,
) : Codec<T> {

    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: T): Pair<ByteArray, FormatCode> {
        val text  = json.encodeToString(serializer, value).toByteArray(Charsets.UTF_8)
        return if (oid == Oid.JSONB) {
            val buffer = ByteArray(text.size + 1)
            buffer[0] = 1
            text.copyInto(buffer, destinationOffset = 1)
            Pair(buffer, FormatCode.BINARY)
        } else {
            Pair(text, FormatCode.BINARY)
        }
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): T {
        val text = if (sourceOid == Oid.JSONB)
            String(bytes, 1, bytes.size - 1, Charsets.UTF_8)
        else
            String(bytes, Charsets.UTF_8)
        return json.decodeFromString(serializer, text)
    }
}
