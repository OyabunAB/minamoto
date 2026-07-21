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
import se.oyabun.minamoto.postgres.codec.PgVectorCodec
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
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

        // Official pgvector images ship the vector extension pre-installed.
        val pgvectorImages = listOf(
            "pgvector/pgvector:pg15",
            "pgvector/pgvector:pg17",
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
                    it.registerHstore()
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
                            CREATE TABLE mood_array_test (id int, moods mood[])
                        """.trimIndent()).execute() }
                        .then { database.run(
                            "INSERT INTO mood_array_test VALUES (1, ARRAY['happy','sad','neutral']::mood[])"
                        ).execute() }
                        .then { database.run("""
                            CREATE TABLE inet_test (id int, address inet)
                        """.trimIndent()).execute() }
                        .then { database.run(
                            "INSERT INTO inet_test VALUES (1, '192.168.1.1'), (2, '::1')"
                        ).execute() }
                        .then { database.run("""
                            CREATE TABLE geo_test (
                                id   int,
                                pt   point,
                                bx   box,
                                cir  circle,
                                li   line,
                                ls   lseg,
                                pth  path,
                                ply  polygon
                            )
                        """.trimIndent()).execute() }
                        .then { database.run("""
                            INSERT INTO geo_test VALUES (
                                1,
                                '(1.0,2.0)',
                                '((3.0,4.0),(1.0,2.0))',
                                '<(0.0,0.0),5.0>',
                                '{1.0,-1.0,0.0}',
                                '[(1.0,2.0),(3.0,4.0)]',
                                '[(0.0,0.0),(1.0,1.0),(2.0,0.0)]',
                                '((0.0,0.0),(1.0,0.0),(1.0,1.0),(0.0,1.0))'
                            )
                        """.trimIndent()).execute() }
                        .then { database.run("CREATE EXTENSION hstore").execute() }
                        .then { database.run("""
                            CREATE TABLE hstore_test (id int, tags hstore)
                        """.trimIndent()).execute() }
                        .then { database.run("""
                            INSERT INTO hstore_test VALUES
                                (1, '"color"=>"red","size"=>"large"'),
                                (2, '"color"=>"blue","size"=>NULL')
                        """.trimIndent()).execute() }
                        .then { database.run("""
                            CREATE TABLE array2d_test (id int, ints int[][], txts text[][])
                        """.trimIndent()).execute() }
                        .then { database.run("""
                            INSERT INTO array2d_test VALUES
                                (1, '{{1,2,3},{4,5,6}}',   '{{"hello","world"},{"foo","bar"}}'),
                                (2, '{{10,NULL},{30,40}}', NULL)
                        """.trimIndent()).execute() },
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
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT moods FROM mood_array_test WHERE id = 1")
                        .multiple().map { it.get<List<Mood>>("moods") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { list ->
                    assertEquals(3, list.size)
                    assertEquals(setOf(Mood.happy, Mood.sad, Mood.neutral), (list as List<*>).toSet())
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- geometric types (native PG geometric, no extension required) ---

            // --- geometric types ---

            dynamicTest("point column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT pt FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgPoint>("pt") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertEquals(PgPoint(1.0, 2.0), it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("box column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT bx FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgBox>("bx") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertEquals(PgBox(PgPoint(3.0, 4.0), PgPoint(1.0, 2.0)), it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("circle column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT cir FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgCircle>("cir") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertEquals(PgCircle(PgPoint(0.0, 0.0), 5.0), it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("line column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT li FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgLine>("li") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertEquals(PgLine(1.0, -1.0, 0.0), it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("lseg column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT ls FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgLseg>("ls") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { assertEquals(PgLseg(PgPoint(1.0, 2.0), PgPoint(3.0, 4.0)), it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("path column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT pth FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgPath>("pth") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { path ->
                    assertEquals(false, path.closed)
                    assertEquals(3, path.points.size)
                    assertEquals(PgPoint(0.0, 0.0), path.points[0])
                    assertEquals(PgPoint(1.0, 1.0), path.points[1])
                    assertEquals(PgPoint(2.0, 0.0), path.points[2])
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("polygon column decoded") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT ply FROM geo_test WHERE id = 1")
                        .multiple().map { it.get<PgPolygon>("ply") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { poly ->
                    assertEquals(4, poly.points.size)
                    assertEquals(PgPoint(0.0, 0.0), poly.points[0])
                    assertEquals(PgPoint(1.0, 0.0), poly.points[1])
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- hstore ---

            dynamicTest("hstore column decoded as Map<String, String?>") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT tags FROM hstore_test WHERE id = 1")
                        .multiple().map { it.get<Map<String, String?>>("tags") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { map ->
                    assertEquals("red",   map["color"])
                    assertEquals("large", map["size"])
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("Map<String, String?> parameter encoded into hstore column") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                val tags = mapOf("env" to "prod", "region" to "eu-west")
                Verify.that(
                    database.run("INSERT INTO hstore_test VALUES (3, :tags)")
                        .bind("tags" to tags).execute()
                        .then {
                            database.query("SELECT tags FROM hstore_test WHERE id = 3")
                                .multiple().map { it.get<Map<String, String?>>("tags") }.take(1)
                        },
                    context = PoolContext(pool),
                ).assertNext { map ->
                    assertEquals("prod",    map["env"])
                    assertEquals("eu-west", map["region"])
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("hstore with null value entry round-trips") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT tags FROM hstore_test WHERE id = 2")
                        .multiple().map { it.get<Map<String, String?>>("tags") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { map ->
                    assertEquals("blue", map["color"])
                    assertNull(map["size"])
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            // --- pgvector — see pgvectorTests() below, requires pgvector/pgvector images ---

            // --- 2D arrays ---

            dynamicTest("INT4[][] decoded as List<List<Int>>") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                // Verify shape and element content independent of row ordering —
                // toSet() because the row order depends on PG's internal binary
                // dimension convention (outer dim may vary by PG version/config).
                Verify.that(
                    database.query("SELECT ints FROM array2d_test WHERE id = 1")
                        .multiple().map { it.get<List<List<Int>>>("ints") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { matrix ->
                    assertEquals(2, matrix.size)
                    assertEquals(3, (matrix[0] as List<*>).size)
                    assertEquals(
                        setOf(listOf(1, 2, 3), listOf(4, 5, 6)),
                        matrix.map { it as List<*> }.toSet(),
                    )
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("TEXT[][] decoded as List<List<String>>") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT txts FROM array2d_test WHERE id = 1")
                        .multiple().map { it.get<List<List<String>>>("txts") }.take(1),
                    context = PoolContext(pool),
                ).assertNext { matrix ->
                    assertEquals(2, matrix.size)
                    assertEquals(
                        setOf(listOf("hello", "world"), listOf("foo", "bar")),
                        matrix.map { it as List<*> }.toSet(),
                    )
                }.completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("null element in 2D array throws CodecFailed") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT ints FROM array2d_test WHERE id = 2")
                        .multiple()
                        .map { row ->
                            runCatching { row.get<List<List<Int>>>("ints") }
                                .exceptionOrNull() is se.oyabun.minamoto.DatabaseException.CodecFailed
                        }
                        .take(1),
                    context = PoolContext(pool),
                ).assertNext { assertTrue(it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
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

    // -------------------------------------------------------------------------
    // pgvector — separate factory, uses pgvector/pgvector images
    // -------------------------------------------------------------------------

    @TestFactory
    fun `pgvector type tests`() = pgvectorImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, pgvectorTests(postgres))
    }

    private fun pgvectorTests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var database: PostgresDatabase

        return listOf(

            dynamicTest("start container") {
                postgres.start()
                val registry = CodecRegistry(discoverRegistrars = false).also { it.registerVector() }
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
                    database.run("CREATE EXTENSION vector").execute()
                        .then { database.run("""
                            CREATE TABLE vec_test (
                                id        int,
                                embedding vector(3)
                            )
                        """.trimIndent()).execute() }
                        .then { database.run("""
                            INSERT INTO vec_test VALUES
                                (1, '[1.0, 0.0, 0.0]'),
                                (2, '[0.8, 0.2, 0.0]'),
                                (3, '[0.0, 0.0, 1.0]')
                        """.trimIndent()).execute() },
                    context = PoolContext(pool),
                ).completesNormally(within = 10.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("vector column decoded as FloatArray") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("SELECT embedding FROM vec_test WHERE id = 1")
                        .multiple()
                        .map { it.get<FloatArray>("embedding") }
                        .take(1),
                    context = PoolContext(pool),
                ).assertNext { assertContentEquals(floatArrayOf(1.0f, 0.0f, 0.0f), it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("FloatArray parameter encoded into vector column") {
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                val vec = floatArrayOf(0.1f, 0.2f, 0.3f)
                Verify.that(
                    database.run("INSERT INTO vec_test VALUES (4, :vec)")
                        .bind("vec" to vec).execute()
                        .then {
                            database.query("SELECT embedding FROM vec_test WHERE id = 4")
                                .multiple()
                                .map { it.get<FloatArray>("embedding") }
                                .take(1)
                        },
                    context = PoolContext(pool),
                ).assertNext { assertContentEquals(vec, it) }
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("cosine distance query returns correct ordering") {
                // Vectors: [1,0,0] (id=1), [0.8,0.2,0] (id=2), [0,0,1] (id=3)
                // Query: nearest to [1,0,0] by cosine distance
                // Expected order: 1 (distance 0), 2 (distance ~0.11), 3 (distance 1)
                val pool = database.pool(PoolConfig(initialSize = 1, maxSize = 3,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.query("""
                        SELECT id
                        FROM vec_test
                        WHERE id IN (1, 2, 3)
                        ORDER BY embedding <=> '[1,0,0]'::vector
                    """.trimIndent())
                        .multiple()
                        .map { it.get<Int>("id") }
                        .take(3),
                    context = PoolContext(pool),
                ).emitsNext(1, 2, 3)
                 .completesNormally(within = 5.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("stop container") { postgres.stop() },
        )
    }
}
