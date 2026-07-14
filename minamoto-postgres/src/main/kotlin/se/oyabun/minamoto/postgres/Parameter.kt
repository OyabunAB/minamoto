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

import se.oyabun.minamoto.postgres.codec.FormatCode

/**
 * An encoded query parameter ready for the wire.
 *
 * [Undefined] represents SQL NULL — the server receives -1 for the parameter length.
 * [Defined] carries the encoded bytes and the wire format produced by the codec.
 */
sealed interface Parameter {
    data object Undefined                                              : Parameter
    data class  Defined(val bytes: ByteArray, val format: FormatCode) : Parameter
}

/** An ordered list of encoded parameters ready for the wire. */
typealias Parameters = List<Parameter>
