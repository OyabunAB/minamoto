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
import kotlinx.datetime.toLocalDateTime
import java.math.BigDecimal
import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.OffsetTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds

internal object Oid {
    const val BOOL          = 16
    const val BYTEA         = 17
    const val INT8          = 20
    const val INT2          = 21
    const val INT4          = 23
    const val TEXT          = 25
    const val OID           = 26
    const val NAME          = 19
    const val CSTRING       = 2275
    const val FLOAT4        = 700
    const val FLOAT8        = 701
    const val BPCHAR        = 1042
    const val VARCHAR       = 1043
    const val DATE          = 1082
    const val TIME          = 1083
    const val TIMESTAMP     = 1114
    const val TIMESTAMPTZ   = 1184
    const val INTERVAL      = 1186
    const val TIMETZ        = 1266
    const val NUMERIC       = 1700
    const val UUID          = 2950
    const val JSON          = 114
    const val JSONB         = 3802
    const val INET          = 869

    const val BOOL_ARRAY        = 1000
    const val INT2_ARRAY        = 1005
    const val INT4_ARRAY        = 1007
    const val INT8_ARRAY        = 1016
    const val FLOAT4_ARRAY      = 1021
    const val FLOAT8_ARRAY      = 1022
    const val TEXT_ARRAY        = 1009
    const val VARCHAR_ARRAY     = 1015
    const val BPCHAR_ARRAY      = 1014
    const val DATE_ARRAY        = 1182
    const val TIME_ARRAY        = 1183
    const val TIMESTAMP_ARRAY   = 1115
    const val TIMESTAMPTZ_ARRAY = 1185
    const val TIMETZ_ARRAY      = 1270
    const val INTERVAL_ARRAY    = 1187
    const val UUID_ARRAY        = 2951
    const val NUMERIC_ARRAY     = 1231
    const val BYTEA_ARRAY       = 1001
    const val JSON_ARRAY        = 199
    const val JSONB_ARRAY       = 3807
    const val INET_ARRAY        = 1041
}

/** Microseconds between Unix epoch (1970-01-01) and Postgres epoch (2000-01-01). */
private const val POSTGRES_EPOCH_MICROS = 946_684_800_000_000L

/** Days between Unix epoch (1970-01-01) and Postgres epoch (2000-01-01). */
private const val POSTGRES_EPOCH_DAYS = 10_957

internal object BooleanCodec : Codec<Boolean> {
    override val oid             = Oid.BOOL
    override val type            = Boolean::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Boolean): Pair<ByteArray, FormatCode> =
        Pair(byteArrayOf(if (value) 1 else 0), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): Boolean = bytes[0] != 0.toByte()
}

internal object ShortCodec : Codec<Short> {
    override val oid             = Oid.INT2
    override val type            = Short::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Short): Pair<ByteArray, FormatCode> =
        Pair(ByteBuffer.allocate(2).putShort(value).array(), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): Short =
        ByteBuffer.wrap(bytes).short
}

internal object IntCodec : Codec<Int> {
    override val oid             = Oid.INT4
    override val type            = Int::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Int): Pair<ByteArray, FormatCode> =
        Pair(ByteBuffer.allocate(4).putInt(value).array(), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): Int = when (sourceOid) {
        Oid.INT2 -> ByteBuffer.wrap(bytes).short.toInt()
        else     -> ByteBuffer.wrap(bytes).int
    }
}

internal object LongCodec : Codec<Long> {
    override val oid             = Oid.INT8
    override val type            = Long::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Long): Pair<ByteArray, FormatCode> =
        Pair(ByteBuffer.allocate(8).putLong(value).array(), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): Long = when (sourceOid) {
        Oid.INT2 -> ByteBuffer.wrap(bytes).short.toLong()
        Oid.INT4 -> ByteBuffer.wrap(bytes).int.toLong()
        else     -> ByteBuffer.wrap(bytes).long
    }
}

internal object FloatCodec : Codec<Float> {
    override val oid             = Oid.FLOAT4
    override val type            = Float::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Float): Pair<ByteArray, FormatCode> =
        Pair(ByteBuffer.allocate(4).putFloat(value).array(), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): Float =
        ByteBuffer.wrap(bytes).float
}

internal object DoubleCodec : Codec<Double> {
    override val oid             = Oid.FLOAT8
    override val type            = Double::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Double): Pair<ByteArray, FormatCode> =
        Pair(ByteBuffer.allocate(8).putDouble(value).array(), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): Double = when (sourceOid) {
        Oid.FLOAT4 -> ByteBuffer.wrap(bytes).float.toDouble()
        else       -> ByteBuffer.wrap(bytes).double
    }
}

/**
 * Numeric binary format: int16 ndigits, int16 weight, int16 sign, int16 dscale, then ndigits×int16
 * in base 10000, most-significant first. Weight is the index (0-based) of the first digit group
 * relative to the units position.
 *
 * Encoding uses text to avoid reimplementing base-10000 packing; the server accepts both.
 * Sign 0xC000 is Postgres NaN — decoded as zero since [BigDecimal] has no NaN.
 */
internal object BigDecimalCodec : Codec<BigDecimal> {
    override val oid             = Oid.NUMERIC
    override val type            = BigDecimal::class
    override val preferredFormat = FormatCode.BINARY

    private const val SIGN_NEGATIVE = 0x4000
    private const val SIGN_NAN      = 0xC000
    private const val NBASE         = 10_000

    override fun encode(value: BigDecimal): Pair<ByteArray, FormatCode> =
        Pair(value.toPlainString().toByteArray(Charsets.UTF_8), FormatCode.TEXT)

    override fun decode(bytes: ByteArray, sourceOid: Int): BigDecimal {
        val buffer  = ByteBuffer.wrap(bytes)
        val ndigits = buffer.short.toInt() and 0xFFFF
        val weight  = buffer.short.toInt()
        val sign    = buffer.short.toInt() and 0xFFFF
        val dscale  = buffer.short.toInt() and 0xFFFF

        if (sign == SIGN_NAN) return BigDecimal.ZERO

        var result = BigInteger.ZERO
        repeat(ndigits) {
            val digit = buffer.short.toInt() and 0xFFFF
            result = result.multiply(BigInteger.valueOf(NBASE.toLong()))
                           .add(BigInteger.valueOf(digit.toLong()))
        }

        val fracDigits = ndigits - (weight + 1)
        val decimal    = BigDecimal(result, fracDigits * 4)
            .setScale(dscale, java.math.RoundingMode.HALF_UP)

        return if (sign == SIGN_NEGATIVE) decimal.negate() else decimal
    }
}

internal object StringCodec : Codec<String> {
    override val oid             = Oid.TEXT
    override val type            = String::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: String): Pair<ByteArray, FormatCode> =
        Pair(value.toByteArray(Charsets.UTF_8), FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): String =
        String(bytes, Charsets.UTF_8)
}

internal object ByteArrayCodec : Codec<ByteArray> {
    override val oid             = Oid.BYTEA
    override val type            = ByteArray::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: ByteArray): Pair<ByteArray, FormatCode> =
        Pair(value, FormatCode.BINARY)

    override fun decode(bytes: ByteArray, sourceOid: Int): ByteArray = bytes
}

/** Binary format: 16 bytes — most-significant int64 then least-significant int64. */
internal object UuidCodec : Codec<UUID> {
    override val oid             = Oid.UUID
    override val type            = UUID::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: UUID): Pair<ByteArray, FormatCode> {
        val buffer = ByteBuffer.allocate(16)
        buffer.putLong(value.mostSignificantBits)
        buffer.putLong(value.leastSignificantBits)
        return Pair(buffer.array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): UUID {
        val buffer = ByteBuffer.wrap(bytes)
        return UUID(buffer.long, buffer.long)
    }
}

/** Binary format: int32 days since Postgres epoch (2000-01-01). */
internal object LocalDateCodec : Codec<LocalDate> {
    override val oid             = Oid.DATE
    override val type            = LocalDate::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: LocalDate): Pair<ByteArray, FormatCode> {
        val days = value.toEpochDays() - POSTGRES_EPOCH_DAYS
        return Pair(ByteBuffer.allocate(4).putInt(days).array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): LocalDate {
        val days = ByteBuffer.wrap(bytes).int
        return LocalDate.fromEpochDays(days + POSTGRES_EPOCH_DAYS)
    }
}

/** Binary format: int64 microseconds since midnight. */
internal object LocalTimeCodec : Codec<LocalTime> {
    override val oid             = Oid.TIME
    override val type            = LocalTime::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: LocalTime): Pair<ByteArray, FormatCode> {
        val micros = (value.hour * 3_600_000_000_000L
                    + value.minute * 60_000_000_000L
                    + value.second * 1_000_000_000L
                    + value.nanosecond) / 1_000L
        return Pair(ByteBuffer.allocate(8).putLong(micros).array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): LocalTime {
        val micros = ByteBuffer.wrap(bytes).long
        return LocalTime.fromNanosecondOfDay(micros * 1_000L)
    }
}

/**
 * Binary format: int64 local-time microseconds since midnight + int32 offset seconds.
 * Postgres stores the local time (not UTC) and offset west of UTC separately.
 * Offset is west-positive; Java uses east-positive — signs are flipped.
 */
internal object OffsetTimeCodec : Codec<OffsetTime> {
    override val oid             = Oid.TIMETZ
    override val type            = OffsetTime::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: OffsetTime): Pair<ByteArray, FormatCode> {
        val localMicros   = value.toLocalTime().toNanoOfDay() / 1_000L
        val offsetSeconds = -value.offset.totalSeconds
        val buffer        = ByteBuffer.allocate(12)
        buffer.putLong(localMicros)
        buffer.putInt(offsetSeconds)
        return Pair(buffer.array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): OffsetTime {
        val buffer       = ByteBuffer.wrap(bytes)
        val localMicros  = buffer.long
        val pgOffsetSecs = buffer.int
        val javaOffset   = ZoneOffset.ofTotalSeconds(-pgOffsetSecs)
        val localTime    = java.time.LocalTime.ofNanoOfDay(localMicros * 1_000L)
        return OffsetTime.of(localTime, javaOffset)
    }
}

/** Binary format: int64 microseconds since Postgres epoch (2000-01-01T00:00:00), no timezone. */
internal object LocalDateTimeCodec : Codec<LocalDateTime> {
    override val oid             = Oid.TIMESTAMP
    override val type            = LocalDateTime::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: LocalDateTime): Pair<ByteArray, FormatCode> {
        val dayMicros  = (value.date.toEpochDays() - POSTGRES_EPOCH_DAYS) * 86_400_000_000L
        val timeMicros = (value.hour   * 3_600_000_000L
                        + value.minute *    60_000_000L
                        + value.second *     1_000_000L
                        + value.nanosecond / 1_000L)
        return Pair(ByteBuffer.allocate(8).putLong(dayMicros + timeMicros).array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): LocalDateTime {
        val pgMicros   = ByteBuffer.wrap(bytes).long
        val pgDays     = (pgMicros / 86_400_000_000L).toInt()
        val timeMicros = pgMicros - pgDays * 86_400_000_000L
        val date       = LocalDate.fromEpochDays(pgDays + POSTGRES_EPOCH_DAYS)
        val time       = LocalTime.fromNanosecondOfDay(timeMicros * 1_000L)
        return LocalDateTime(date, time)
    }
}

/**
 * Binary format: int64 microseconds since Postgres epoch in UTC.
 * Postgres always persists timestamptz as UTC regardless of session timezone.
 * Represented as [Instant] — callers apply their own timezone if needed.
 */
internal object InstantCodec : Codec<Instant> {
    override val oid             = Oid.TIMESTAMPTZ
    override val type            = Instant::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Instant): Pair<ByteArray, FormatCode> {
        val epochMicros = value.epochSeconds * 1_000_000L + value.nanosecondsOfSecond / 1_000L
        val pgMicros    = epochMicros - POSTGRES_EPOCH_MICROS
        return Pair(ByteBuffer.allocate(8).putLong(pgMicros).array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): Instant {
        val pgMicros     = ByteBuffer.wrap(bytes).long
        val epochMicros  = pgMicros + POSTGRES_EPOCH_MICROS
        val epochSeconds = epochMicros / 1_000_000L
        val nanoAdjust   = ((epochMicros % 1_000_000L) * 1_000L).toInt()
        return Instant.fromEpochSeconds(epochSeconds, nanoAdjust.toLong())
    }
}

/**
 * Binary format: int64 microseconds + int32 days + int32 months.
 * [Duration] has no month or day concept; those components are folded into the duration on decode.
 * Encode writes zero days and zero months — use only for sub-day durations.
 */
internal object DurationCodec : Codec<Duration> {
    override val oid             = Oid.INTERVAL
    override val type            = Duration::class
    override val preferredFormat = FormatCode.BINARY

    override fun encode(value: Duration): Pair<ByteArray, FormatCode> {
        val buffer = ByteBuffer.allocate(16)
        buffer.putLong(value.inWholeNanoseconds / 1_000L)
        buffer.putInt(0)
        buffer.putInt(0)
        return Pair(buffer.array(), FormatCode.BINARY)
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): Duration {
        val buffer = ByteBuffer.wrap(bytes)
        val micros = buffer.long
        val days   = buffer.int
        return (micros * 1_000L).nanoseconds + (days * 86_400_000_000_000L).nanoseconds
    }
}

/**
 * Binary inet wire format: 1-byte address family (2 = IPv4, 3 = IPv6), 1-byte CIDR prefix
 * length, 1-byte is_cidr flag (0 for inet), 1-byte address byte count, then the raw address.
 * Encodes host addresses (/32 for IPv4, /128 for IPv6); CIDR prefix bits on decode are ignored.
 */
internal object InetAddressCodec : Codec<InetAddress> {
    override val oid             = Oid.INET
    override val type            = InetAddress::class
    override val preferredFormat = FormatCode.BINARY

    private const val AF_INET4: Byte = 2
    private const val AF_INET6: Byte = 3

    override fun encode(value: InetAddress): Pair<ByteArray, FormatCode> {
        val addr   = value.address
        val family = if (addr.size == 4) AF_INET4 else AF_INET6
        val result = ByteArray(4 + addr.size)
        result[0] = family
        result[1] = (addr.size * 8).toByte()  // full host prefix (/32 or /128)
        result[2] = 0                          // is_cidr = false
        result[3] = addr.size.toByte()
        addr.copyInto(result, 4)
        return result to FormatCode.BINARY
    }

    override fun decode(bytes: ByteArray, sourceOid: Int): InetAddress {
        val nb   = bytes[3].toInt() and 0xFF
        val addr = bytes.copyOfRange(4, 4 + nb)
        return InetAddress.getByAddress(addr)
    }
}

internal val builtInCodecs: List<Codec<*>> = listOf(
    BooleanCodec,
    ShortCodec,
    IntCodec,
    LongCodec,
    FloatCodec,
    DoubleCodec,
    BigDecimalCodec,
    StringCodec,
    ByteArrayCodec,
    UuidCodec,
    LocalDateCodec,
    LocalTimeCodec,
    OffsetTimeCodec,
    LocalDateTimeCodec,
    InstantCodec,
    DurationCodec,
    InetAddressCodec,
)
