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
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializerOrNull
import se.oyabun.minamoto.DatabaseException
import java.math.BigDecimal
import java.net.InetAddress
import java.time.OffsetTime
import java.util.ServiceLoader
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.time.Duration

/**
 * Pluggable extension point for registering additional codecs at pool creation time.
 *
 * Implementations are discovered via [ServiceLoader] unless disabled in [CodecRegistry].
 * The registration order of SPI registrars is not guaranteed — do not rely on it.
 */
interface CodecRegistrar {
    fun register(registry: CodecRegistry)
}

/**
 * Lookup table from `(OID, KClass)` to [Codec].
 *
 * Built-in codecs are pre-registered for all standard Postgres types. Additional codecs
 * are added via [register], [registerByType], or through [CodecRegistrar] SPI.
 *
 * **Numeric widening**: requesting `Long` for an `int4` column (or `Double` for `float4`)
 * succeeds without explicit registration. The widen chain is:
 * `Short → Int → Long → BigDecimal` and `Float → Double`.
 *
 * **User-defined types**: call [registerByType] for codecs whose Postgres OID is not known
 * at registration time (e.g. enum types, composite types, domain types). The OID is
 * resolved lazily from the first `RowDescription` that references the type and then cached.
 *
 * **Enum types**: [registerEnum] installs a text-based codec that maps enum constant
 * [names][Enum.name] to Postgres enum labels. The Postgres OID is resolved lazily.
 *
 * **Any dispatch**: `row.get<Any>("column")` resolves to the canonical Kotlin type for
 * the column's OID (e.g. `Int` for `int4`, `Instant` for `timestamptz`). Unknown OIDs
 * fall back to `String` (the text wire representation).
 *
 * **String fallback**: `row.get<String>("column")` always succeeds regardless of OID —
 * columns with no registered string codec are decoded as their text wire representation.
 *
 * **JSON/JSONB**: any type annotated with `@kotlinx.serialization.Serializable` can be
 * registered for OID 114 (json) or 3802 (jsonb) via [registerJson] or [registerJsonb].
 *
 * @param json shared [Json] instance for all JSON codecs registered through this registry
 * @param discoverRegistrars when true (default), [ServiceLoader] discovers [CodecRegistrar]
 *   implementations on the classpath and invokes them during construction
 */
class CodecRegistry(
    val json:               Json    = Json,
    val discoverRegistrars: Boolean = true,
) {
    // Thread-safe: lazy OID binding from connection threads writes to this map at decode time.
    private val codecs         = ConcurrentHashMap<Pair<Int, KClass<*>>, Codec<*>>()

    // Codecs registered without a fixed OID (user-defined types, enums). On first decode
    // the actual OID is learned from RowDescription and the codec is promoted into [codecs].
    private val typeOnlyCodecs = ConcurrentHashMap<KClass<*>, Codec<*>>()

    // Canonical Kotlin type per built-in OID, used by Any dispatch.
    private val defaultTypeByOid: Map<Int, KClass<*>> = mapOf(
        Oid.BOOL        to Boolean::class,
        Oid.INT2        to Short::class,
        Oid.INT4        to Int::class,
        Oid.INT8        to Long::class,
        Oid.FLOAT4      to Float::class,
        Oid.FLOAT8      to Double::class,
        Oid.NUMERIC     to BigDecimal::class,
        Oid.TEXT        to String::class,
        Oid.VARCHAR     to String::class,
        Oid.BPCHAR      to String::class,
        Oid.NAME        to String::class,
        Oid.BYTEA       to ByteArray::class,
        Oid.UUID        to UUID::class,
        Oid.DATE        to LocalDate::class,
        Oid.TIME        to LocalTime::class,
        Oid.TIMETZ      to OffsetTime::class,
        Oid.TIMESTAMP   to LocalDateTime::class,
        Oid.TIMESTAMPTZ to Instant::class,
        Oid.INTERVAL    to Duration::class,
        Oid.JSON        to String::class,
        Oid.JSONB       to String::class,
        Oid.INET        to InetAddress::class,
    )

    init {
        registerBuiltIns()
        if (discoverRegistrars) {
            ServiceLoader.load(CodecRegistrar::class.java).forEach { it.register(this) }
        }
    }

    /** Registers a codec for its primary [Codec.oid] + [Codec.type] pair. */
    fun register(codec: Codec<*>) {
        codecs[codec.oid to codec.type] = codec
    }

    /**
     * Registers a codec under an additional OID without changing its primary [Codec.oid].
     * Used to cover OID aliases (e.g. varchar → StringCodec) and widen targets.
     */
    fun register(oid: Int, codec: Codec<*>) {
        codecs[oid to codec.type] = codec
    }

    /**
     * Registers a codec without a fixed Postgres OID. Intended for user-defined types
     * (enum types, composite types, domain types) whose OID is assigned per-database and
     * therefore unknown at pool-creation time.
     *
     * On the first decode of a column whose OID has no direct registry entry, the registry
     * checks [typeOnlyCodecs] by Kotlin type and, if a match is found, promotes the codec
     * under the actual OID for O(1) future lookups.
     *
     * [Codec.oid] on the provided codec is ignored — set it to `0` or any sentinel value.
     */
    fun registerByType(codec: Codec<*>) {
        typeOnlyCodecs[codec.type] = codec
    }

    /**
     * Registers [T] for the json OID (114) using the [Json] instance held by this registry.
     * Fails at call time if [T] has no kotlinx.serialization serializer.
     */
    inline fun <reified T : Any> registerJson() = registerJson(T::class)

    /**
     * Registers [T] for the jsonb OID (3802) using the [Json] instance held by this registry.
     * Fails at call time if [T] has no kotlinx.serialization serializer.
     */
    inline fun <reified T : Any> registerJsonb() = registerJsonb(T::class)

    /**
     * Installs a text-based codec for [E] that maps enum constant [names][Enum.name]
     * to Postgres enum labels and back via [enumValueOf].
     *
     * The Postgres OID is resolved lazily on first decode. The Postgres enum label must
     * match the Kotlin constant name exactly (case-sensitive). If your Postgres labels
     * differ in case from your Kotlin constants, provide a custom codec via [registerByType].
     */
    inline fun <reified E : Enum<E>> registerEnum() {
        registerByType(object : Codec<E> {
            override val oid             = 0
            override val type            = E::class
            override val preferredFormat = FormatCode.TEXT

            override fun encode(value: E): Pair<ByteArray, FormatCode> =
                value.name.toByteArray(Charsets.UTF_8) to FormatCode.TEXT

            override fun decode(bytes: ByteArray, sourceOid: Int): E =
                enumValueOf(bytes.toString(Charsets.UTF_8))
        })
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> registerJson(type: KClass<T>) {
        val serializer = requireSerializer(type) as kotlinx.serialization.KSerializer<T>
        val codec      = JsonCodec(Oid.JSON, type, serializer, json)
        codecs[Oid.JSON to type] = codec
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> registerJsonb(type: KClass<T>) {
        val serializer = requireSerializer(type) as kotlinx.serialization.KSerializer<T>
        val codec      = JsonCodec(Oid.JSONB, type, serializer, json)
        codecs[Oid.JSONB to type] = codec
    }

    /**
     * Returns the preferred wire format for a given OID.
     * Falls back to TEXT if no codec is registered for the OID.
     */
    fun preferredFormat(oid: Int): FormatCode =
        codecs.entries.firstOrNull { (key, _) -> key.first == oid }
            ?.value?.preferredFormat
            ?: FormatCode.TEXT

    /**
     * Returns the codec for [oid] + [type], applying the following resolution order:
     *
     * 1. **Any dispatch** — if [type] is [Any], resolve via [defaultTypeByOid] then recurse,
     *    falling back to any codec registered for [oid] or ultimately to [StringCodec].
     * 2. **Exact hit** — direct `(oid, type)` map lookup.
     * 3. **Numeric widening** — `Short → Int → Long → BigDecimal`, `Float → Double`.
     * 4. **Type-only fallback** — user-defined type codecs registered via [registerByType];
     *    the codec is lazily promoted to a fixed-OID entry for O(1) future lookups.
     * 5. **String fallback** — any OID can be decoded as [String] (text wire representation).
     *
     * Throws [DatabaseException.CodecFailed] only when none of the above match.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> find(oid: Int, type: KClass<T>): Codec<T> {
        // 1. Any dispatch — resolve to the canonical Kotlin type for this OID
        if (type == Any::class) {
            val defaultType = defaultTypeByOid[oid]
            if (defaultType != null) return find(oid, defaultType) as Codec<T>
            // Unknown OID: return any codec registered for this OID, else String
            codecs.entries.firstOrNull { (key, _) -> key.first == oid }
                ?.let { return it.value as Codec<T> }
            return StringCodec as Codec<T>
        }

        // 2. Exact hit
        codecs[oid to type]?.let { return it as Codec<T> }

        // 3. Numeric widening
        widenedCodec(oid, type)?.let { return it as Codec<T> }

        // 4. Type-only fallback for user-defined types; lazily bind OID for future lookups
        typeOnlyCodecs[type]?.let { codec ->
            val typed = codec as Codec<T>
            codecs.putIfAbsent(oid to type, typed)
            return typed
        }

        // 5. String fallback — any PG type can be decoded as its text wire representation
        if (type == String::class) return StringCodec as Codec<T>

        throw DatabaseException.CodecFailed("no codec for OID $oid → ${type.simpleName}")
    }

    /**
     * Returns the codec for encoding [value] to send as a query parameter.
     * Resolution order:
     * 1. Exact [KClass] match in the fixed-OID registry.
     * 2. Type-only codecs ([registerByType]) by exact type.
     * 3. Supertype match in the fixed-OID registry — handles concrete subclasses of
     *    abstract codec types (e.g. [java.net.Inet4Address] → [java.net.InetAddress]).
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> findForEncoding(value: T): Codec<T> {
        val type = value::class as KClass<T>
        codecs.entries
            .firstOrNull { (key, _) -> key.second == type }
            ?.let { return it.value as Codec<T> }
        typeOnlyCodecs[type]?.let { return it as Codec<T> }
        // Supertype fallback: Inet4Address / Inet6Address → InetAddressCodec, etc.
        codecs.entries
            .firstOrNull { (key, _) -> (key.second as KClass<*>).java.isAssignableFrom(type.java) }
            ?.let { return it.value as Codec<T> }
        throw DatabaseException.CodecFailed("no codec for type ${type.simpleName}")
    }

    private fun widenedCodec(oid: Int, target: KClass<*>): Codec<*>? {
        val numericWiden = mapOf<KClass<*>, List<KClass<*>>>(
            Long::class             to listOf(Int::class, Short::class),
            Int::class              to listOf(Short::class),
            Double::class           to listOf(Float::class),
            BigDecimal::class       to listOf(Long::class, Int::class, Short::class),
        )
        val sources = numericWiden[target] ?: return null
        for (source in sources) {
            codecs[oid to source]?.let { return it }
        }
        return null
    }

    private fun registerBuiltIns() {
        builtInCodecs.forEach { register(it) }

        // OID aliases for String
        listOf(Oid.VARCHAR, Oid.BPCHAR, Oid.NAME, Oid.CSTRING, Oid.OID).forEach {
            register(it, StringCodec)
        }

        // Widen targets for numeric OIDs
        register(Oid.INT4, LongCodec)
        register(Oid.INT2, LongCodec)
        register(Oid.INT2, IntCodec)
        register(Oid.FLOAT4, DoubleCodec)

        builtInCodecs.forEach { elementCodec ->
            val arrayOid = arrayOidByElementOid[elementCodec.oid] ?: return@forEach
            register(ArrayCodec(arrayOid, elementCodec.oid, elementCodec))
        }
    }

    private fun <T : Any> requireSerializer(type: KClass<T>) =
        serializerOrNull(type.java)
            ?: throw DatabaseException.CodecFailed(
                "${type.simpleName} has no kotlinx.serialization serializer — annotate with @Serializable"
            )
}
