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

import kotlin.reflect.KClass

/**
 * Wire format requested from the server for result columns, and used when sending parameters.
 *
 * PostgreSQL accepts 0 (text) or 1 (binary) per parameter or result column in the Bind message.
 * Binary is smaller, unambiguous, and requires no text parsing — prefer it for all types that
 * have a stable binary representation. Fall back to text only where the binary format is
 * undocumented or server-version-dependent (e.g. `numeric` in some edge cases).
 */
enum class FormatCode(val wire: Short) {
    TEXT(0),
    BINARY(1),
}

/**
 * Converts between a Kotlin value of type [T] and the PostgreSQL wire representation.
 *
 * A codec is registered for a primary [oid] + [type] pair. The [oid] is the primary OID
 * this codec handles; additional OIDs (e.g. array OIDs or aliases) are handled by
 * registering the codec under multiple OIDs in [PgCodecRegistry].
 *
 * [encode] produces the raw bytes and the format code to use in the `Bind` message.
 * [decode] receives the raw bytes from a `DataRow` and the actual OID from `RowDescription`
 * — the OID is passed through so multi-OID codecs can adjust behaviour if needed.
 *
 * Implementations must be stateless and thread-safe.
 */
interface Codec<T : Any> {
    val oid:             Int
    val type:            KClass<T>
    val preferredFormat: FormatCode

    fun encode(value: T): Pair<ByteArray, FormatCode>

    /**
     * Decode [bytes] into a [T].
     *
     * [sourceOid] is the OID from the server's RowDescription — may differ from [oid]
     * when the codec is registered for multiple OIDs (e.g. `int4` and `int8` both widen to `Long`).
     */
    fun decode(bytes: ByteArray, sourceOid: Int): T
}
