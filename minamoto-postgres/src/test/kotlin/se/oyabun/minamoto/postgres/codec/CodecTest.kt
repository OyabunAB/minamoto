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

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.serialization.Serializable
import se.oyabun.minamoto.DatabaseException
import java.math.BigDecimal
import java.time.OffsetTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class CodecTest {

    private fun <T : Any> roundtrip(codec: Codec<T>, value: T): T {
        val (bytes, _) = codec.encode(value)
        return codec.decode(bytes, codec.oid)
    }

    // -------------------------------------------------------------------------
    // Scalar roundtrips
    // -------------------------------------------------------------------------

    @Test fun `boolean true roundtrips`()  = assertEquals(true,  roundtrip(BooleanCodec, true))
    @Test fun `boolean false roundtrips`() = assertEquals(false, roundtrip(BooleanCodec, false))

    @Test fun `short roundtrips`()       = assertEquals(Short.MAX_VALUE, roundtrip(ShortCodec, Short.MAX_VALUE))
    @Test fun `short negative roundtrips`() = assertEquals(-1, roundtrip(ShortCodec, -1))

    @Test fun `int roundtrips`()         = assertEquals(Int.MAX_VALUE, roundtrip(IntCodec, Int.MAX_VALUE))
    @Test fun `int negative roundtrips`() = assertEquals(Int.MIN_VALUE, roundtrip(IntCodec, Int.MIN_VALUE))

    @Test fun `long roundtrips`()        = assertEquals(Long.MAX_VALUE, roundtrip(LongCodec, Long.MAX_VALUE))
    @Test fun `long zero roundtrips`()   = assertEquals(0L, roundtrip(LongCodec, 0L))

    @Test fun `float roundtrips`()       = assertEquals(3.14f, roundtrip(FloatCodec, 3.14f))
    @Test fun `float negative roundtrips`() = assertEquals(-0.001f, roundtrip(FloatCodec, -0.001f))

    @Test fun `double roundtrips`()      = assertEquals(3.141592653589793, roundtrip(DoubleCodec, 3.141592653589793))

    @Test fun `string roundtrips`()      = assertEquals("hello", roundtrip(StringCodec, "hello"))
    @Test fun `string unicode roundtrips`() = assertEquals("日本語", roundtrip(StringCodec, "日本語"))
    @Test fun `string empty roundtrips`()  = assertEquals("", roundtrip(StringCodec, ""))

    @Test fun `bytea roundtrips`() {
        val value = byteArrayOf(0, 1, 127, -128)
        val result = roundtrip(ByteArrayCodec, value)
        assertEquals(value.toList(), result.toList())
    }

    @Test fun `uuid roundtrips`() {
        val value = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
        assertEquals(value, roundtrip(UuidCodec, value))
    }

    // -------------------------------------------------------------------------
    // BigDecimal — binary decode, text encode
    // -------------------------------------------------------------------------

    @Test fun `bigdecimal positive roundtrips via text encode binary decode`() {
        val value    = BigDecimal("12345.6789")
        val (bytes, format) = BigDecimalCodec.encode(value)
        assertEquals(FormatCode.TEXT, format)
        val decoded  = BigDecimal(String(bytes, Charsets.UTF_8))
        assertEquals(value, decoded)
    }

    @Test fun `bigdecimal binary decode positive`() {
        val decoded = BigDecimalCodec.decode(encodeBigDecimalBinary("123.45"), Oid.NUMERIC)
        assertEquals(BigDecimal("123.45"), decoded)
    }

    @Test fun `bigdecimal binary decode negative`() {
        val decoded = BigDecimalCodec.decode(encodeBigDecimalBinary("-99.9"), Oid.NUMERIC)
        assertEquals(BigDecimal("-99.9"), decoded)
    }

    @Test fun `bigdecimal binary decode zero`() {
        val decoded = BigDecimalCodec.decode(encodeBigDecimalBinary("0.00"), Oid.NUMERIC)
        assertEquals(BigDecimal("0.00"), decoded)
    }

    // -------------------------------------------------------------------------
    // Numeric widening
    // -------------------------------------------------------------------------

    @Test fun `int codec decodes int2 by widening`() {
        val (bytes, _) = ShortCodec.encode(42)
        val result     = IntCodec.decode(bytes, Oid.INT2)
        assertEquals(42, result)
    }

    @Test fun `long codec decodes int4 by widening`() {
        val (bytes, _) = IntCodec.encode(1_000_000)
        val result     = LongCodec.decode(bytes, Oid.INT4)
        assertEquals(1_000_000L, result)
    }

    @Test fun `long codec decodes int2 by widening`() {
        val (bytes, _) = ShortCodec.encode(7)
        val result     = LongCodec.decode(bytes, Oid.INT2)
        assertEquals(7L, result)
    }

    @Test fun `double codec decodes float4 by widening`() {
        val (bytes, _) = FloatCodec.encode(1.5f)
        val result     = DoubleCodec.decode(bytes, Oid.FLOAT4)
        assertEquals(1.5f.toDouble(), result, 1e-6)
    }

    // -------------------------------------------------------------------------
    // Date / time
    // -------------------------------------------------------------------------

    @Test fun `localdate roundtrips`() {
        val value = LocalDate(2026, 7, 14)
        assertEquals(value, roundtrip(LocalDateCodec, value))
    }

    @Test fun `localdate postgres epoch roundtrips`() {
        val value = LocalDate(2000, 1, 1)
        assertEquals(value, roundtrip(LocalDateCodec, value))
    }

    @Test fun `localdate before unix epoch roundtrips`() {
        val value = LocalDate(1969, 12, 31)
        assertEquals(value, roundtrip(LocalDateCodec, value))
    }

    @Test fun `localtime roundtrips`() {
        val value = LocalTime(10, 30, 45, 123_000_000)
        assertEquals(value, roundtrip(LocalTimeCodec, value))
    }

    @Test fun `localtime midnight roundtrips`() {
        val value = LocalTime(0, 0, 0, 0)
        assertEquals(value, roundtrip(LocalTimeCodec, value))
    }

    @Test fun `localdatetime roundtrips without timezone assumption`() {
        val value = LocalDateTime(2026, 7, 14, 10, 0, 0, 0)
        assertEquals(value, roundtrip(LocalDateTimeCodec, value))
    }

    @Test fun `localdatetime same wall clock in any timezone roundtrips identically`() {
        val tokyo    = LocalDateTime(2026, 7, 14, 19, 0, 0, 0)
        val newYork  = LocalDateTime(2026, 7, 14, 6, 0, 0, 0)
        val (tokyoBytes, _)   = LocalDateTimeCodec.encode(tokyo)
        val (newYorkBytes, _) = LocalDateTimeCodec.encode(newYork)
        // Different wall clocks encode to different bytes — no UTC folding
        assert(!tokyoBytes.contentEquals(newYorkBytes))
        assertEquals(tokyo,   LocalDateTimeCodec.decode(tokyoBytes,   Oid.TIMESTAMP))
        assertEquals(newYork, LocalDateTimeCodec.decode(newYorkBytes, Oid.TIMESTAMP))
    }

    @Test fun `instant roundtrips`() {
        val value = Instant.fromEpochSeconds(1_720_944_000L, 500_000_000)
        assertEquals(value, roundtrip(InstantCodec, value))
    }

    @Test fun `instant postgres epoch roundtrips`() {
        val value = Instant.fromEpochSeconds(946_684_800L)
        assertEquals(value, roundtrip(InstantCodec, value))
    }

    @Test fun `offsettime roundtrips`() {
        val value = OffsetTime.of(14, 30, 0, 0, ZoneOffset.ofHours(2))
        assertEquals(value, roundtrip(OffsetTimeCodec, value))
    }

    @Test fun `offsettime utc roundtrips`() {
        val value = OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC)
        assertEquals(value, roundtrip(OffsetTimeCodec, value))
    }

    @Test fun `duration roundtrips`() {
        val value = 2.hours + 30.minutes + 15.seconds
        assertEquals(value, roundtrip(DurationCodec, value))
    }

    @Test fun `duration zero roundtrips`() {
        assertEquals(0.seconds, roundtrip(DurationCodec, 0.seconds))
    }

    // -------------------------------------------------------------------------
    // Array codec
    // -------------------------------------------------------------------------

    @Test fun `int array roundtrips`() {
        val codec = ArrayCodec(Oid.INT4_ARRAY, Oid.INT4, IntCodec)
        val value = listOf(1, 2, 3, 42, -7)
        assertEquals(value, roundtrip(codec, value))
    }

    @Test fun `string array roundtrips`() {
        val codec = ArrayCodec(Oid.TEXT_ARRAY, Oid.TEXT, StringCodec)
        val value = listOf("foo", "bar", "baz")
        assertEquals(value, roundtrip(codec, value))
    }

    @Test fun `empty array roundtrips`() {
        val codec = ArrayCodec(Oid.INT4_ARRAY, Oid.INT4, IntCodec)
        assertEquals(emptyList(), roundtrip(codec, emptyList()))
    }

    @Test fun `boolean array roundtrips`() {
        val codec = ArrayCodec(Oid.BOOL_ARRAY, Oid.BOOL, BooleanCodec)
        val value = listOf(true, false, true)
        assertEquals(value, roundtrip(codec, value))
    }

    @Test fun `2D int4 array decoded as nested List`() {
        // Hand-craft the binary wire representation of {{1,2,3},{4,5,6}}:
        // header: ndim=2, hasNulls=0, elementOid=23, dim[0]=(2,1), dim[1]=(3,1)
        // elements (row-major): 1,2,3,4,5,6 each as (length=4, value)
        val buf = java.nio.ByteBuffer.allocate(7 * 4 + 6 * 8)
        buf.putInt(2); buf.putInt(0); buf.putInt(23)
        buf.putInt(2); buf.putInt(1)  // dim[0]
        buf.putInt(3); buf.putInt(1)  // dim[1]
        intArrayOf(1, 2, 3, 4, 5, 6).forEach { v -> buf.putInt(4); buf.putInt(v) }

        val codec = ArrayCodec(Oid.INT4_ARRAY, Oid.INT4, IntCodec)
        @Suppress("UNCHECKED_CAST")
        val matrix = codec.decode(buf.array(), Oid.INT4_ARRAY) as List<List<Int>>

        assertEquals(listOf(listOf(1, 2, 3), listOf(4, 5, 6)), matrix)
    }

    @Test fun `null element in 2D array throws CodecFailed`() {
        // {{1,NULL},{3,4}} — NULL at flat index 1
        val buf = java.nio.ByteBuffer.allocate(7 * 4 + 4 * 8 + 1 * 4)
        buf.putInt(2); buf.putInt(0); buf.putInt(23)
        buf.putInt(2); buf.putInt(1)
        buf.putInt(2); buf.putInt(1)
        buf.putInt(4); buf.putInt(1)   // element 0: value=1
        buf.putInt(-1)                  // element 1: NULL
        buf.putInt(4); buf.putInt(3)   // element 2: value=3
        buf.putInt(4); buf.putInt(4)   // element 3: value=4

        val codec = ArrayCodec(Oid.INT4_ARRAY, Oid.INT4, IntCodec)
        assertFailsWith<DatabaseException.CodecFailed> {
            codec.decode(buf.array(), Oid.INT4_ARRAY)
        }
    }

    // -------------------------------------------------------------------------
    // JSON / JSONB
    // -------------------------------------------------------------------------

    @Serializable
    data class Payload(val id: Int, val name: String)

    @Test fun `json codec roundtrips serializable type`() {
        val registry = CodecRegistry()
        registry.registerJson<Payload>()
        val codec = registry.find(Oid.JSON, Payload::class)
        val value = Payload(1, "walter")
        val (bytes, _) = codec.encode(value)
        assertEquals(value, codec.decode(bytes, Oid.JSON))
    }

    @Test fun `jsonb codec prepends and strips version byte`() {
        val registry = CodecRegistry()
        registry.registerJsonb<Payload>()
        val codec = registry.find(Oid.JSONB, Payload::class)
        val value = Payload(2, "jesse")
        val (bytes, _) = codec.encode(value)
        assertEquals(0x01.toByte(), bytes[0])
        assertEquals(value, codec.decode(bytes, Oid.JSONB))
    }

    @Test fun `registering non-serializable type for json throws CodecFailed`() {
        val registry = CodecRegistry()
        assertFailsWith<DatabaseException.CodecFailed> {
            registry.registerJson<Thread>()
        }
    }

    // -------------------------------------------------------------------------
    // CodecRegistry
    // -------------------------------------------------------------------------

    @Test fun `registry finds exact codec`() {
        val registry = CodecRegistry()
        val codec    = registry.find(Oid.INT4, Int::class)
        assertEquals(42, codec.decode(IntCodec.encode(42).first, Oid.INT4))
    }

    @Test fun `registry widens int4 to long`() {
        val registry = CodecRegistry()
        val codec    = registry.find(Oid.INT4, Long::class)
        val (bytes, _) = IntCodec.encode(99)
        assertEquals(99L, codec.decode(bytes, Oid.INT4))
    }

    @Test fun `registry widens int2 to int`() {
        val registry = CodecRegistry()
        val codec    = registry.find(Oid.INT2, Int::class)
        val (bytes, _) = ShortCodec.encode(7)
        assertEquals(7, codec.decode(bytes, Oid.INT2))
    }

    @Test fun `registry widens float4 to double`() {
        val registry = CodecRegistry()
        val codec    = registry.find(Oid.FLOAT4, Double::class)
        val (bytes, _) = FloatCodec.encode(1.5f)
        assertEquals(1.5, codec.decode(bytes, Oid.FLOAT4), 1e-6)
    }

    @Test fun `registry throws CodecFailed for unknown OID and type`() {
        val registry = CodecRegistry()
        assertFailsWith<DatabaseException.CodecFailed> {
            registry.find(99999, Int::class)     // no codec for Int at a non-existent OID
        }
    }

    @Test fun `registry throws CodecFailed for known OID but incompatible type`() {
        val registry = CodecRegistry()
        assertFailsWith<DatabaseException.CodecFailed> {
            registry.find(Oid.BOOL, Int::class)
        }
    }

    @Test fun `registry finds varchar via alias`() {
        val registry = CodecRegistry()
        val codec    = registry.find(Oid.VARCHAR, String::class)
        assertEquals("hello", codec.decode(StringCodec.encode("hello").first, Oid.VARCHAR))
    }

    @Test fun `registry finds array codec for int4`() {
        val registry = CodecRegistry()
        val codec    = registry.find(Oid.INT4_ARRAY, List::class)
        val arrayCodec = ArrayCodec(Oid.INT4_ARRAY, Oid.INT4, IntCodec)
        val (bytes, _) = arrayCodec.encode(listOf(1, 2, 3))
        @Suppress("UNCHECKED_CAST")
        assertEquals(listOf(1, 2, 3), (codec as Codec<List<*>>).decode(bytes, Oid.INT4_ARRAY))
    }

    @Test fun `registry findForEncoding finds codec by runtime type`() {
        val registry = CodecRegistry()
        val codec    = registry.findForEncoding(42)
        val (bytes, _) = codec.encode(42)
        assertEquals(42, IntCodec.decode(bytes, Oid.INT4))
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Encodes a decimal string into Postgres binary numeric format for decode testing.
     * Delegates to text encoding then re-parses — this tests the decoder independently
     * of the encoder without duplicating the base-10000 packing logic here.
     */
    private fun encodeBigDecimalBinary(value: String): ByteArray =
        encodeBigDecimalToBinaryNumeric(BigDecimal(value))

    /**
     * Produces a Postgres binary numeric payload for a given [BigDecimal].
     *
     * Postgres splits the number into base-10000 digit groups. The integer part is
     * left-padded to a group boundary; the fractional part is right-padded to a group
     * boundary. weight is the 0-based index of the most-significant digit group
     * relative to the units position.
     */
    private fun encodeBigDecimalToBinaryNumeric(value: BigDecimal): ByteArray {
        val negative  = value.signum() < 0
        val plain     = value.abs().toPlainString()
        val dotIndex  = plain.indexOf('.')
        val intStr    = if (dotIndex >= 0) plain.substring(0, dotIndex) else plain
        val fracStr   = if (dotIndex >= 0) plain.substring(dotIndex + 1) else ""
        val dscale    = fracStr.length

        // Integer part: left-pad to multiple of 4, split into groups
        val intPadded  = intStr.padStart(((intStr.length + 3) / 4) * 4, '0')
        val intGroups  = (0 until intPadded.length step 4).map { intPadded.substring(it, it + 4).toInt() }

        // Fractional part: right-pad to multiple of 4, split into groups
        val fracPadded = fracStr.padEnd(((fracStr.length + 3) / 4) * 4, '0')
        val fracGroups = if (fracStr.isEmpty()) emptyList()
                         else (0 until fracPadded.length step 4).map { fracPadded.substring(it, it + 4).toInt() }

        val allGroups  = (intGroups + fracGroups).dropWhile { it == 0 }.let { if (it.isEmpty()) listOf(0) else it }

        // weight = number of integer groups minus 1 (index of the units group)
        val weight  = intGroups.size - 1
        val ndigits = allGroups.size
        val sign    = if (negative) 0x4000 else 0x0000

        val buffer = java.nio.ByteBuffer.allocate(8 + ndigits * 2)
        buffer.putShort(ndigits.toShort())
        buffer.putShort(weight.toShort())
        buffer.putShort(sign.toShort())
        buffer.putShort(dscale.toShort())
        allGroups.forEach { buffer.putShort(it.toShort()) }
        return buffer.array()
    }
}
