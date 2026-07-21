package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.None
import se.oyabun.aelv.Verify
import se.oyabun.aelv.map
import se.oyabun.aelv.take
import se.oyabun.aelv.then
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Tier 5 — extended type coverage tests.
 *
 * Covers inet, user-defined enum types, and the Any-dispatch path that returns the
 * canonical Kotlin type for a column without an explicit type parameter at the call site.
 */
class ExtendedTypeIntegrationTest {

    companion object {
        // Lowercase constants to match Postgres enum label convention.
        @Suppress("EnumEntryName")
        enum class Mood { happy, sad, neutral }

        val postgresImages = listOf(
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `extended type tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, tests(postgres))
    }

    private fun tests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var database: PostgresDatabase

        return listOf(

            dynamicTest("start container") {
                postgres.start()
                val registry = CodecRegistry(discoverRegistrars = false).also {
                    it.registerEnum<Mood>()
                }
                database = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                ), registry = registry)
            },

            dynamicTest("setup DDL") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.run("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')").execute()
                        .then { database.run("""
                            CREATE TABLE mood_test (id int, mood mood)
                        """.trimIndent()).execute() }
                        .then { database.run(
                            "INSERT INTO mood_test VALUES (1, 'happy'), (2, 'sad'), (3, 'neutral')"
                        ).execute() }
                        .then { database.run("""
                            CREATE TABLE inet_test (id int, address inet)
                        """.trimIndent()).execute() }
                        .then { database.run(
                            "INSERT INTO inet_test VALUES (1, '192.168.1.1'), (2, '::1')"
                        ).execute() },
                    context = PoolContext(pool),
                ).completesNormally(within = 10.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- inet ---

            dynamicTest("inet column decoded as InetAddress") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT address FROM inet_test WHERE id = 1")
                        .multiple().map { it.get<InetAddress>("address") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<Inet4Address>(it); assertEquals("192.168.1.1", it.hostAddress) }
                 .completesNormally(within = 5.seconds)
                Verify.that(
                    database.query("SELECT address FROM inet_test WHERE id = 2")
                        .multiple().map { it.get<InetAddress>("address") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<Inet6Address>(it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("InetAddress parameter encoded into inet column") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                val addr = InetAddress.getByName("10.0.0.1")
                Verify.that(
                    database.run("INSERT INTO inet_test VALUES (3, :addr)")
                        .bind("addr" to addr).execute()
                        .then {
                            database.query("SELECT address FROM inet_test WHERE id = 3")
                                .multiple()
                                .map { it.get<InetAddress>("address") }
                                .take(1)
                        },
                    context = PoolContext(pool),
                ).assertNext { assertEquals("10.0.0.1", it.hostAddress) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("inet[] decoded as List<InetAddress>") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT ARRAY['192.168.0.1'::inet, '10.0.0.1'::inet] AS addrs")
                        .multiple()
                        .map { it.get<List<InetAddress>>("addrs") }
                        .take(1),
                    context = PoolContext(pool),
                ).assertNext { list ->
                    assertEquals(2, list.size)
                    assertEquals("192.168.0.1", list[0].hostAddress)
                    assertEquals("10.0.0.1",   list[1].hostAddress)
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- user-defined enum ---

            dynamicTest("Kotlin enum registered for PG enum OID encodes and decodes") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT mood FROM mood_test ORDER BY id")
                        .multiple()
                        .map { it.get<Mood>("mood") }
                        .take(3),
                    context = PoolContext(pool),
                ).emitsNext(Mood.happy, Mood.sad, Mood.neutral)
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("Kotlin enum encodes as parameter and round-trips") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.run("INSERT INTO mood_test VALUES (4, :mood)")
                        .bind("mood" to Mood.neutral).execute()
                        .then {
                            database.query("SELECT mood FROM mood_test WHERE id = 4")
                                .multiple()
                                .map { it.get<Mood>("mood") }
                                .take(1)
                        },
                    context = PoolContext(pool),
                ).assertNext { assertEquals(Mood.neutral, it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("PG enum column decoded as String without explicit codec") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT mood FROM mood_test WHERE id = 1")
                        .multiple()
                        .map { it.get<String>("mood") }
                        .take(1),
                    context = PoolContext(pool),
                ).assertNext { assertEquals("happy", it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- Any dispatch ---

            dynamicTest("bool column read as Any returns Boolean") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT true AS v").multiple()
                        .map { it.get<Any>("v") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertTrue(it is Boolean); assertEquals(true, it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("int4 column read as Any returns Int") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT 42::int4 AS v").multiple()
                        .map { it.get<Any>("v") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<Int>(it); assertEquals(42, it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("text column read as Any returns String") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT 'hello'::text AS v").multiple()
                        .map { it.get<Any>("v") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<String>(it); assertEquals("hello", it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("uuid column read as Any returns UUID") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT gen_random_uuid() AS v").multiple()
                        .map { it.get<Any>("v") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<java.util.UUID>(it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("timestamptz column read as Any returns Instant") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT now() AS v").multiple()
                        .map { it.get<Any>("v") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<kotlinx.datetime.Instant>(it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("json column read as Any returns String") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("""SELECT '{"ok":true}'::json AS v""").multiple()
                        .map { it.get<Any>("v") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<String>(it); assertTrue((it as String).contains("ok")) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("inet column read as Any returns InetAddress") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT address FROM inet_test WHERE id = 1")
                        .multiple()
                        .map { it.get<Any>("address") }
                        .take(1),
                    context = PoolContext(pool),
                ).assertNext { assertIs<InetAddress>(it); assertEquals("192.168.1.1", (it as InetAddress).hostAddress) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- enum array ---

            dynamicTest("ENUM[] decoded as List<Mood>") {
                Assumptions.abort<Unit>("not yet implemented")
            },

            // --- geometric types (native PG geometric, no extension required) ---

            dynamicTest("point column decoded") { Assumptions.abort<Unit>("not yet implemented") },
            dynamicTest("box column decoded")   { Assumptions.abort<Unit>("not yet implemented") },
            dynamicTest("circle column decoded") { Assumptions.abort<Unit>("not yet implemented") },
            dynamicTest("line column decoded")   { Assumptions.abort<Unit>("not yet implemented") },
            dynamicTest("lseg column decoded")   { Assumptions.abort<Unit>("not yet implemented") },
            dynamicTest("path column decoded")   { Assumptions.abort<Unit>("not yet implemented") },
            dynamicTest("polygon column decoded") { Assumptions.abort<Unit>("not yet implemented") },

            // --- hstore (requires hstore extension) ---

            dynamicTest("hstore column decoded as Map<String, String>") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("Map<String, String> parameter encoded into hstore column") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("hstore with null value entry round-trips") {
                Assumptions.abort<Unit>("not yet implemented")
            },

            // --- pgvector (requires vector extension) ---

            dynamicTest("vector column decoded as FloatArray") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("FloatArray parameter encoded into vector column") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("cosine distance query returns correct ordering") {
                Assumptions.abort<Unit>("not yet implemented")
            },

            // --- 2D arrays ---

            dynamicTest("INT4[][] decoded as List<List<Int>>") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("TEXT[][] decoded as List<List<String>>") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("null element in 2D array throws CodecFailed") {
                Assumptions.abort<Unit>("not yet implemented")
            },

            // --- COPY IN ---

            dynamicTest("COPY FROM STDIN with binary stream inserts rows") {
                Assumptions.abort<Unit>("not yet implemented")
            },
            dynamicTest("COPY row count matches inserted data") {
                Assumptions.abort<Unit>("not yet implemented")
            },

            dynamicTest("stop container") { postgres.stop() },
        )
    }
}
