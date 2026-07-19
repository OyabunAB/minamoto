package se.oyabun.minamoto.postgres

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Verify
import se.oyabun.aelv.map
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

class ArrayAndJsonCodecIntegrationTest {

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
    fun `array and JSON codec tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, tests(postgres))
    }

    private fun connect(postgres: PostgreSQLContainer, registry: CodecRegistry = CodecRegistry()): Pair<PostgresDatabase, MinamotoPool> {
        val database = PostgresDatabase(
            config   = ConnectionConfig(
                host     = postgres.host,
                port     = postgres.firstMappedPort,
                user     = postgres.username,
                password = { postgres.password },
                database = postgres.databaseName,
            ),
            registry = registry,
        )
        val pool = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 5, validation = ValidationQuery.None))
        return Pair(database, pool)
    }

    private fun tests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var database: PostgresDatabase
        lateinit var pool: MinamotoPool
        return listOf(

            dynamicTest("start container") {
                postgres.start()
                val (db, p) = connect(postgres)
                database = db
                pool = p
            },

            // --- Array decoding ---

            dynamicTest("INT4[] column decoded as List<Int>") {
                Verify.that(
                    database.query("SELECT ARRAY[1, 2, 3]::int4[] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<Int>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(1, 2, 3), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("INT8[] column decoded as List<Long>") {
                Verify.that(
                    database.query("SELECT ARRAY[1, 2, 3]::int8[] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<Long>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(1L, 2L, 3L), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("FLOAT8[] column decoded as List<Double>") {
                Verify.that(
                    database.query("SELECT ARRAY[1.1, 2.2, 3.3]::float8[] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<Double>
                    }, context = PoolContext(pool),
                ).assertNext { result ->
                    assertEquals(3, result.size)
                    assertEquals(1.1, result[0], 0.001)
                }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("BOOL[] column decoded as List<Boolean>") {
                Verify.that(
                    database.query("SELECT ARRAY[true, false, true]::bool[] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<Boolean>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(true, false, true), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("TEXT[] column decoded as List<String>") {
                Verify.that(
                    database.query("SELECT ARRAY['foo', 'bar', 'baz']::text[] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<String>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf("foo", "bar", "baz"), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("UUID[] column decoded as List<UUID>") {
                Verify.that(
                    database.query("SELECT ARRAY['550e8400-e29b-41d4-a716-446655440000']::uuid[] AS v")
                        .single().map { row ->
                            @Suppress("UNCHECKED_CAST")
                            row.get<List<*>>("v") as List<UUID>
                        }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("TIMESTAMP[] column decoded as List<LocalDateTime>") {
                Verify.that(
                    database.query("SELECT ARRAY['2026-07-14T10:00:00'::timestamp] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<LocalDateTime>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(LocalDateTime(2026, 7, 14, 10, 0, 0)), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("TIMESTAMPTZ[] column decoded as List<Instant>") {
                Verify.that(
                    database.query("SELECT ARRAY['2026-07-14T10:00:00Z'::timestamptz] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<Instant>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(Instant.parse("2026-07-14T10:00:00Z")), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("NUMERIC[] column decoded as List<BigDecimal>") {
                Verify.that(
                    database.query("SELECT ARRAY[1.5, 2.5]::numeric[] AS v").single().map { row ->
                        @Suppress("UNCHECKED_CAST")
                        row.get<List<*>>("v") as List<BigDecimal>
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(BigDecimal("1.5"), BigDecimal("2.5")), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("empty array round-trips as empty List") {
                Verify.that(
                    database.query("SELECT ARRAY[]::int4[] AS v").single().map { row -> row.get<List<*>>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(emptyList<Int>(), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("single-element array round-trips") {
                Verify.that(
                    database.query("SELECT ARRAY[42]::int4[] AS v").single().map { row -> row.get<List<*>>("v") }, context = PoolContext(pool),
                ).assertNext { assertEquals(listOf(42), it) }.completesNormally(within = TEST_TIMEOUT)
            },
            dynamicTest("null element in array throws CodecFailed") {
                Verify.that(
                    database.query("SELECT ARRAY[1, NULL, 3]::int4[] AS v").single()
                        .map { row -> row.get<List<*>>("v") }, context = PoolContext(pool),
                ).failedWith<DatabaseException.CodecFailed>(within = TEST_TIMEOUT)
            },

            // --- JSON codecs ---

            dynamicTest("json column decoded as @Serializable data class") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val (db, p) = connect(postgres, registry)
                Verify.that(
                    db.query("SELECT '{\"id\":1,\"name\":\"walter\"}'::json AS v")
                        .single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(Payload(1, "walter"), it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },
            dynamicTest("jsonb column decoded as @Serializable data class") {
                val registry = CodecRegistry()
                registry.registerJsonb<Payload>()
                val (db, p) = connect(postgres, registry)
                Verify.that(
                    db.query("SELECT '{\"id\":2,\"name\":\"jesse\"}'::jsonb AS v")
                        .single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(Payload(2, "jesse"), it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },
            dynamicTest("@Serializable parameter encoded into json column") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val payload = Payload(3, "skyler")
                val (db, p) = connect(postgres, registry)
                Verify.that(
                    db.query("SELECT :v::json AS v").bind("v" to payload).single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(payload, it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },
            dynamicTest("@Serializable parameter encoded into jsonb column") {
                val registry = CodecRegistry()
                registry.registerJsonb<Payload>()
                val payload = Payload(4, "hank")
                val (db, p) = connect(postgres, registry)
                Verify.that(
                    db.query("SELECT :v::jsonb AS v").bind("v" to payload).single().map { row -> row.get<Payload>("v") }, context = PoolContext(p),
                ).assertNext { assertEquals(payload, it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },
            dynamicTest("json null column returns null via getOrNull") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val (db, p) = connect(postgres, registry)
                Verify.that(
                    db.query("SELECT NULL::json AS v").single(), context = PoolContext(p),
                ).assertNext { row -> assertNull(row.getOrNull<Payload>("v")) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },

            dynamicTest("stop container") {
                Verify.that(pool.close()).completesNormally()
                postgres.stop()
            },
        )
    }
}
