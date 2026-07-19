package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Verify
import se.oyabun.aelv.discard
import se.oyabun.aelv.fold
import se.oyabun.aelv.map
import se.oyabun.aelv.take
import se.oyabun.aelv.then
import se.oyabun.aelv.toMany
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ProtocolIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `protocol integration tests across postgres versions`() = postgresImages.map { image ->
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
                pool = database.pool(PoolConfig(initialSize = 2, minIdle = 1, maxSize = 5,
                    validation = ValidationQuery.None))
                Verify.that(
                    database.run("CREATE TABLE proto_test (id SERIAL PRIMARY KEY, val TEXT NOT NULL UNIQUE)").execute()
                        .then { database.run("CREATE TABLE proto_fk_parent (id INT PRIMARY KEY)").execute() }
                        .then { database.run("CREATE TABLE proto_fk_child (parent_id INT REFERENCES proto_fk_parent(id))").execute() }
                        .then { database.run("CREATE USER limited_user PASSWORD 'pass' NOSUPERUSER").execute() },
                    context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("unique constraint violation sqlState 23505 surfaces as UniqueViolation") {
                Verify.that(
                    database.run("INSERT INTO proto_test (val) VALUES ('dup')").execute()
                        .then { database.run("INSERT INTO proto_test (val) VALUES ('dup')").execute() },
                    context = PoolContext(pool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.UniqueViolation>(it) }
            },

            dynamicTest("not-null violation sqlState 23502 surfaces as NotNullViolation") {
                Verify.that(
                    database.run("INSERT INTO proto_test (val) VALUES (NULL)").execute(),
                    context = PoolContext(pool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.NotNullViolation>(it) }
            },

            dynamicTest("foreign key violation sqlState 23503 surfaces as ForeignKeyViolation") {
                Verify.that(
                    database.run("INSERT INTO proto_fk_child (parent_id) VALUES (999)").execute(),
                    context = PoolContext(pool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.ForeignKeyViolation>(it) }
            },

            dynamicTest("undefined table sqlState 42P01 surfaces as UndefinedTable") {
                Verify.that(
                    database.query("SELECT * FROM no_such_table").multiple().discard(),
                    context = PoolContext(pool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.UndefinedTable>(it) }
            },

            dynamicTest("undefined column sqlState 42703 surfaces as UndefinedColumn") {
                Verify.that(
                    database.query("SELECT no_such_col FROM proto_test").multiple().discard(),
                    context = PoolContext(pool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.UndefinedColumn>(it) }
            },

            dynamicTest("permission denied sqlState 42501 surfaces as PermissionDenied") {
                val limitedDb   = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = "limited_user",
                    password = { "pass" },
                    database = postgres.databaseName,
                ))
                val limitedPool = limitedDb.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 1,
                    validation = ValidationQuery.None))
                Verify.that(
                    limitedDb.query("SELECT * FROM proto_test").multiple().discard(),
                    context = PoolContext(limitedPool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.PermissionDenied>(it) }
                Verify.that(limitedPool.close()).completesNormally()
            },

            dynamicTest("syntax error sqlState 42601 surfaces as SyntaxError") {
                Verify.that(
                    database.query("SELEKT 1").multiple().discard(),
                    context = PoolContext(pool),
                ).completesWithError(within = TEST_TIMEOUT).also { assertIs<DatabaseException.SyntaxError>(it) }
            },

            dynamicTest("large result set streams via PortalSuspended across multiple Execute rounds") {
                Verify.that(
                    database.run(
                        "INSERT INTO proto_test (val) SELECT 'row_' || g FROM generate_series(1,200) g"
                    ).execute(),
                    context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)
                Verify.that(
                    database.query("SELECT count(*) AS n FROM proto_test").single().map { it.get<Long>("n") },
                    context = PoolContext(pool),
                ).assertNext { assertTrue(it >= 200) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("fetchSize 1 streams one row per Execute round-trip") {
                Verify.that(
                    database.query("SELECT val FROM proto_test LIMIT 5").multiple()
                        .fold(0) { acc, _ -> acc + 1 },
                    context = PoolContext(pool),
                ).assertNext { assertEquals(5, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("cancelling stream mid-flight leaves connection usable") {
                Verify.that(
                    database.query("SELECT val FROM proto_test").multiple()
                        .take(1).fold(0) { acc, _ -> acc + 1 },
                    context = PoolContext(pool),
                ).assertNext { assertEquals(1, it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(
                    database.query("SELECT 1 AS n").single().map { it.get<Int>("n") },
                    context = PoolContext(pool),
                ).assertNext { assertEquals(1, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("INSERT RETURNING delivers generated id as row") {
                Verify.that(
                    database.query("INSERT INTO proto_test (val) VALUES ('ret1') RETURNING id")
                        .single().map { it.get<Int>("id") },
                    context = PoolContext(pool),
                ).assertNext { assertTrue(it > 0) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("UPDATE RETURNING delivers updated columns") {
                Verify.that(
                    database.query("UPDATE proto_test SET val = 'upd_' || id WHERE id = 1 RETURNING id, val")
                        .single().map { it.get<String>("val") },
                    context = PoolContext(pool),
                ).assertNext { assertTrue(it.startsWith("upd_")) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("INSERT RETURNING with multiple columns delivers full row") {
                Verify.that(
                    database.query("INSERT INTO proto_test (val) VALUES ('ret2') RETURNING id, val")
                        .single().map { row -> row.get<Int>("id") to row.get<String>("val") },
                    context = PoolContext(pool),
                ).assertNext { (id, v) -> assertTrue(id > 0); assertEquals("ret2", v) }
                    .completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("SET timezone updates serverParameters on next query") {
                val tzDatabase = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                    timezone = "America/New_York",
                ))
                val tzPool = tzDatabase.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 1,
                    validation = ValidationQuery.None))
                Verify.that(
                    tzDatabase.query("SHOW timezone").single().map { it.get<String>("TimeZone") },
                    context = PoolContext(tzPool),
                ).assertNext { assertEquals("America/New_York", it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(tzPool.close()).completesNormally()
            },

            dynamicTest("server RAISE NOTICE is surfaced without failing the query") {
                Verify.that(
                    database.run("DO $$ BEGIN RAISE NOTICE 'test notice'; END $$").execute(),
                    context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("stop container") { postgres.stop() },
        )
    }
}
