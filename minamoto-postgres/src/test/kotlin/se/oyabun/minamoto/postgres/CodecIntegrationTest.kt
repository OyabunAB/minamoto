package se.oyabun.minamoto.postgres

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Many
import se.oyabun.aelv.Verify
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.map
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.transactionally
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import java.math.BigDecimal
import java.time.OffsetTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class CodecIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )

        @Serializable
        data class Payload(val id: Int, val name: String)
    }

    @TestFactory
    fun `codec integration across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, tests(postgres))
    }

    private fun tests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var database: PostgresDatabase
        lateinit var pool: MinamotoPool
        return listOf(

            dynamicTest("start container") {
                postgres.start()
                database = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                ))
                pool = database.pool(PoolConfig(initialSize = 2, minIdle = 2, maxSize = 5, validation = ValidationQuery.None))
            },

            // --- Scalar decoding ---

            dynamicTest("bool column decoded as Boolean") {
                Verify.that(
                    database.query("SELECT true::bool AS v").single().map { it.get<Boolean>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(true, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("int2 column decoded as Short") {
                Verify.that(
                    database.query("SELECT 32767::int2 AS v").single().map { it.get<Short>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(Short.MAX_VALUE, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("int4 column decoded as Int") {
                Verify.that(
                    database.query("SELECT 2147483647::int4 AS v").single().map { it.get<Int>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(Int.MAX_VALUE, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("int8 column decoded as Long") {
                Verify.that(
                    database.query("SELECT 9223372036854775807::int8 AS v").single().map { it.get<Long>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(Long.MAX_VALUE, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("int4 column widened to Long") {
                Verify.that(
                    database.query("SELECT 42::int4 AS v").single().map { it.get<Long>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(42L, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("int2 column widened to Int") {
                Verify.that(
                    database.query("SELECT 7::int2 AS v").single().map { it.get<Int>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(7, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("float4 column decoded as Float") {
                Verify.that(
                    database.query("SELECT 3.14::float4 AS v").single().map { it.get<Float>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(3.14f, it, 0.001f) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("float8 column decoded as Double") {
                Verify.that(
                    database.query("SELECT 3.141592653589793::float8 AS v").single().map { it.get<Double>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(3.141592653589793, it, 1e-9) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("float4 column widened to Double") {
                Verify.that(
                    database.query("SELECT 1.5::float4 AS v").single().map { it.get<Double>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(1.5, it, 0.001) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("numeric column decoded as BigDecimal") {
                Verify.that(
                    database.query("SELECT 123.45::numeric AS v").single().map { it.get<BigDecimal>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(BigDecimal("123.45"), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("text column decoded as String") {
                Verify.that(
                    database.query("SELECT 'hello'::text AS v").single().map { it.get<String>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals("hello", it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("varchar column decoded as String") {
                Verify.that(
                    database.query("SELECT 'world'::varchar(10) AS v").single().map { it.get<String>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals("world", it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("bpchar column decoded as String") {
                Verify.that(
                    database.query("SELECT 'x'::bpchar AS v").single().map { it.get<String>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals("x", it.trim()) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("bytea column decoded as ByteArray") {
                Verify.that(
                    database.query("SELECT '\\xDEADBEEF'::bytea AS v").single().map { it.get<ByteArray>("v") }, context = PoolContext(pool),
                ).assertNext {
                    assertEquals(
                        listOf<Byte>(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte()),
                        it.toList(),
                    )
                }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("uuid column decoded as UUID") {
                Verify.that(
                    database.query("SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS v")
                        .single().map { it.get<UUID>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("date column decoded as LocalDate") {
                Verify.that(
                    database.query("SELECT '2026-07-14'::date AS v").single().map { it.get<LocalDate>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(LocalDate(2026, 7, 14), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("time column decoded as LocalTime") {
                Verify.that(
                    database.query("SELECT '10:30:00'::time AS v").single().map { it.get<LocalTime>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(LocalTime(10, 30, 0), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("timetz column decoded as OffsetTime") {
                Verify.that(
                    database.query("SELECT '14:30:00+02:00'::timetz AS v").single().map { it.get<OffsetTime>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(OffsetTime.of(14, 30, 0, 0, ZoneOffset.ofHours(2)), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("timestamp column decoded as LocalDateTime") {
                Verify.that(
                    database.query("SELECT '2026-07-14T10:00:00'::timestamp AS v").single().map { it.get<LocalDateTime>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(LocalDateTime(2026, 7, 14, 10, 0, 0), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("timestamptz column decoded as Instant") {
                Verify.that(
                    database.query("SELECT '2026-07-14T10:00:00Z'::timestamptz AS v").single().map { it.get<Instant>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(Instant.parse("2026-07-14T10:00:00Z"), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("interval column decoded as Duration") {
                Verify.that(
                    database.query("SELECT '2 hours 30 minutes'::interval AS v").single().map { it.get<kotlin.time.Duration>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(2.hours + 30.minutes, it) }.completes(within = TEST_TIMEOUT)
            },

            // --- Null handling ---

            dynamicTest("SQL NULL with getOrNull returns null") {
                Verify.that(
                    database.query("SELECT NULL::text AS v").single(), context = PoolContext(pool),
                ).assertNext { row -> assertNull(row.getOrNull<String>("v")) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("SQL NULL with get throws UnexpectedNull") {
                Verify.that(
                    database.query("SELECT NULL::text AS v").single().map { row -> row.get<String>("v") }, context = PoolContext(pool),
                ).failsWith<DatabaseException.UnexpectedNull>(within = TEST_TIMEOUT)
            },
            dynamicTest("non-null value with getOrNull returns value") {
                Verify.that(
                    database.query("SELECT 'hello'::text AS v").single(), context = PoolContext(pool),
                ).assertNext { row -> assertEquals("hello", row.getOrNull<String>("v")) }.completes(within = TEST_TIMEOUT)
            },

            // --- Parameter binding ---

            dynamicTest("Boolean parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::bool AS v").bind("v" to true).single().map { it.get<Boolean>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(true, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("Int parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::int4 AS v").bind("v" to 42).single().map { it.get<Int>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(42, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("Long parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::int8 AS v").bind("v" to 9999L).single().map { it.get<Long>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(9999L, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("Double parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::float8 AS v").bind("v" to 3.14).single().map { it.get<Double>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(3.14, it, 1e-9) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("String parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::text AS v").bind("v" to "minamoto").single().map { it.get<String>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals("minamoto", it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("UUID parameter round-tripped") {
                val uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
                Verify.that(
                    database.query("SELECT :v::uuid AS v").bind("v" to uuid).single().map { it.get<UUID>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(uuid, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("LocalDate parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::date AS v").bind("v" to LocalDate(2026, 7, 14)).single().map { it.get<LocalDate>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(LocalDate(2026, 7, 14), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("LocalTime parameter round-tripped") {
                Verify.that(
                    database.query("SELECT :v::time AS v").bind("v" to LocalTime(10, 30, 0)).single().map { it.get<LocalTime>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(LocalTime(10, 30, 0), it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("LocalDateTime parameter round-tripped") {
                val dt = LocalDateTime(2026, 7, 14, 10, 0, 0)
                Verify.that(
                    database.query("SELECT :v::timestamp AS v").bind("v" to dt).single().map { it.get<LocalDateTime>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(dt, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("Instant parameter round-tripped") {
                val instant = Instant.parse("2026-07-14T10:00:00Z")
                Verify.that(
                    database.query("SELECT :v::timestamptz AS v").bind("v" to instant).single().map { it.get<Instant>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(instant, it) }.completes(within = TEST_TIMEOUT)
            },

            // --- DML row counts ---

            dynamicTest("INSERT returns rowsUpdated = 1") {
                Verify.that(
                    transactionally {
                        database.modify("CREATE TEMP TABLE IF NOT EXISTS rows_test (id int)").count()
                            .flatMap { database.modify("INSERT INTO rows_test VALUES (1)").count() }
                            .toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(1L, it) }.completes(within = TEST_TIMEOUT)
            },
            dynamicTest("DELETE returns rowsUpdated = 0 when no rows matched") {
                Verify.that(
                    transactionally {
                        database.modify("CREATE TEMP TABLE IF NOT EXISTS empty_test (id int)").count()
                            .flatMap { database.modify("DELETE FROM empty_test WHERE id = 999").count() }
                            .toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(0L, it) }.completes(within = TEST_TIMEOUT)
            },

            // --- Server errors surface typed exceptions ---

            dynamicTest("server error surfaces UndefinedTable for unknown table") {
                Verify.that(
                    database.query("SELECT * FROM nonexistent_table_xyz").multiple(), context = PoolContext(pool),
                ).failsWith<DatabaseException.UndefinedTable>(within = TEST_TIMEOUT)
            },
            dynamicTest("unique constraint violation surfaces UniqueViolation") {
                Verify.that(
                    transactionally {
                        database.modify("CREATE TEMP TABLE IF NOT EXISTS uniq_test (id int PRIMARY KEY)").count()
                            .flatMap { database.modify("INSERT INTO uniq_test VALUES (1)").count() }
                            .flatMap { database.modify("INSERT INTO uniq_test VALUES (1)").count() }
                            .toMany()
                    }, context = PoolContext(pool),
                ).failsWith<DatabaseException.UniqueViolation>(within = TEST_TIMEOUT)
            },

            // --- JSON codecs ---

            dynamicTest("json column decoded as @Serializable data class") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val db = PostgresDatabase(
                    config   = ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = { postgres.password },
                        database = postgres.databaseName,
                    ),
                    registry = registry,
                )
                val p = db.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3, validation = ValidationQuery.None))
                Verify.that(
                    db.query("SELECT '{\"id\":1,\"name\":\"walter\"}'::json AS v")
                        .single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(Payload(1, "walter"), it) }.completes(within = TEST_TIMEOUT)
                Verify.that(p.close()).completes()
            },
            dynamicTest("jsonb column decoded as @Serializable data class") {
                val registry = CodecRegistry()
                registry.registerJsonb<Payload>()
                val db = PostgresDatabase(
                    config   = ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = { postgres.password },
                        database = postgres.databaseName,
                    ),
                    registry = registry,
                )
                val p = db.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3, validation = ValidationQuery.None))
                Verify.that(
                    db.query("SELECT '{\"id\":2,\"name\":\"jesse\"}'::jsonb AS v")
                        .single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(Payload(2, "jesse"), it) }.completes(within = TEST_TIMEOUT)
                Verify.that(p.close()).completes()
            },
            dynamicTest("@Serializable parameter encoded into json column") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val payload = Payload(3, "skyler")
                val db = PostgresDatabase(
                    config   = ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = { postgres.password },
                        database = postgres.databaseName,
                    ),
                    registry = registry,
                )
                val p = db.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3, validation = ValidationQuery.None))
                Verify.that(
                    db.query("SELECT :v::json AS v").bind("v" to payload).single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(payload, it) }.completes(within = TEST_TIMEOUT)
                Verify.that(p.close()).completes()
            },
            dynamicTest("json null column returns null via getOrNull") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val db = PostgresDatabase(
                    config   = ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = { postgres.password },
                        database = postgres.databaseName,
                    ),
                    registry = registry,
                )
                val p = db.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3, validation = ValidationQuery.None))
                Verify.that(
                    db.query("SELECT NULL::json AS v").single(), context = PoolContext(p),
                ).assertNext { row -> assertNull(row.getOrNull<Payload>("v")) }.completes(within = TEST_TIMEOUT)
                Verify.that(p.close()).completes()
            },

            dynamicTest("stop container") {
                Verify.that(pool.close()).completes()
                postgres.stop()
            },
        )
    }
}
