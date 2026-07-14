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

import kotlinx.serialization.json.Json
import kotlinx.serialization.serializerOrNull
import se.oyabun.minamoto.MinamotoException
import java.util.ServiceLoader
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.typeOf

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
 * are added via [register] or through [CodecRegistrar] SPI.
 *
 * **Numeric widening**: requesting `Long` for an `int4` column (or `Double` for `float4`)
 * succeeds without explicit registration. The widen chain is:
 * `Short → Int → Long → BigDecimal` and `Float → Double`.
 *
 * **JSON/JSONB**: any type annotated with `@kotlinx.serialization.Serializable` can be
 * registered for OID 114 (json) or 3802 (jsonb) via [registerJson] or [registerJsonb].
 * The [json] instance supplied here is used for all JSON codecs — configure it with
 * custom serializers before constructing the pool.
 *
 * @param json shared [Json] instance for all JSON codecs registered through this registry
 * @param discoverRegistrars when true (default), [ServiceLoader] discovers [CodecRegistrar]
 *   implementations on the classpath and invokes them during construction
 */
class CodecRegistry(
    val json:               Json    = Json,
    val discoverRegistrars: Boolean = true,
) {
    private val codecs = mutableMapOf<Pair<Int, KClass<*>>, Codec<*>>()

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
     * Registers [T] for the json OID (114) using the [Json] instance held by this registry.
     * Fails at call time if [T] has no kotlinx.serialization serializer.
     */
    inline fun <reified T : Any> registerJson() = registerJson(T::class)

    /**
     * Registers [T] for the jsonb OID (3802) using the [Json] instance held by this registry.
     * Fails at call time if [T] has no kotlinx.serialization serializer.
     */
    inline fun <reified T : Any> registerJsonb() = registerJsonb(T::class)

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
     * Returns the codec for [oid] + [type], applying numeric widening if no exact match exists.
     *
     * Throws [MinamotoException.CodecFailed] when no codec can be found or widened to.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> find(oid: Int, type: KClass<T>): Codec<T> {
        codecs[oid to type]?.let { return it as Codec<T> }

        val widened = widenedCodec(oid, type)
            ?: throw MinamotoException.CodecFailed("no codec for OID $oid → ${type.simpleName}")

        return widened as Codec<T>
    }

    /**
     * Returns the codec for encoding [value] to send as a query parameter.
     * Looks up by runtime type, falling back to widening.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> findForEncoding(value: T): Codec<T> {
        val type = value::class as KClass<T>
        codecs.entries
            .firstOrNull { (key, _) -> key.second == type }
            ?.let { return it.value as Codec<T> }
        throw MinamotoException.CodecFailed("no codec for type ${type.simpleName}")
    }

    private fun widenedCodec(oid: Int, target: KClass<*>): Codec<*>? {
        val numericWiden = mapOf<KClass<*>, List<KClass<*>>>(
            Long::class       to listOf(Int::class, Short::class),
            Int::class        to listOf(Short::class),
            Double::class     to listOf(Float::class),
            java.math.BigDecimal::class to listOf(Long::class, Int::class, Short::class),
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
            ?: throw MinamotoException.CodecFailed(
                "${type.simpleName} has no kotlinx.serialization serializer — annotate with @Serializable"
            )
}
