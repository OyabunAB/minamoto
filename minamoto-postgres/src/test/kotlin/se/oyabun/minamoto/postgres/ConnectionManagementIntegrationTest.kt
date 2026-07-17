package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Many
import se.oyabun.aelv.Verify
import se.oyabun.aelv.delaySubscription
import se.oyabun.aelv.flatMapNone
import se.oyabun.aelv.map
import se.oyabun.aelv.merge
import se.oyabun.aelv.then
import se.oyabun.aelv.toMany
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ConnectionManagementIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `connection management tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, tests(postgres))
    }

    private fun tests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var pool: MinamotoPool
        return listOf(

            dynamicTest("start container") { postgres.start() },

            // --- Session parameters ---

            dynamicTest("search_path set in StartupMessage is active on connect") {
                val database = PostgresDatabase(ConnectionConfig(
                    host       = postgres.host,
                    port       = postgres.firstMappedPort,
                    user       = postgres.username,
                    password   = { postgres.password },
                    database   = postgres.databaseName,
                    searchPath = listOf("myschema", "public"),
                ))
                val p = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3,
                    acquireTimeout = 5.seconds, createTimeout = 10.seconds, validation = ValidationQuery.None))
                Verify.that(
                    database.query("SHOW search_path").single().map { it.get<String>("search_path") }, context = PoolContext(p),
                ).assertNext { searchPath ->
                    assertTrue(searchPath.contains("myschema"),
                        "expected search_path to contain myschema but was: $searchPath")
                }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },

            dynamicTest("timezone set in StartupMessage is active on connect") {
                val database = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                    timezone = "UTC",
                ))
                val p = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3,
                    acquireTimeout = 5.seconds, createTimeout = 10.seconds, validation = ValidationQuery.None))
                Verify.that(
                    database.query("SHOW timezone").single().map { it.get<String>("TimeZone") }, context = PoolContext(p),
                ).assertNext { assertEquals("UTC", it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },

            dynamicTest("applicationName appears in pg_stat_activity") {
                val database = PostgresDatabase(ConnectionConfig(
                    host            = postgres.host,
                    port            = postgres.firstMappedPort,
                    user            = postgres.username,
                    password        = { postgres.password },
                    database        = postgres.databaseName,
                    applicationName = "test-app",
                ))
                val p = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3,
                    acquireTimeout = 5.seconds, createTimeout = 10.seconds, validation = ValidationQuery.None))
                Verify.that(
                    database.query(
                        "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
                    ).single().map { it.get<String>("application_name") }, context = PoolContext(p),
                ).assertNext { assertEquals("test-app", it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },

            // --- ParameterStatus (verified via SHOW, not internal fields) ---

            dynamicTest("server_version ParameterStatus received on connect") {
                val database = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                ))
                val p = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 3,
                    acquireTimeout = 5.seconds, createTimeout = 10.seconds, validation = ValidationQuery.None))
                Verify.that(
                    database.query("SHOW server_version").single().map { it.get<String>("server_version") }, context = PoolContext(p),
                ).assertNext { version ->
                    assertTrue(version.isNotEmpty(), "server_version was empty")
                }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },

            dynamicTest("SET timezone reflected in subsequent query") {
                val database = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                ))
                // maxSize = 1 forces same connection for SET and SHOW
                val p = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 1,
                    acquireTimeout = 5.seconds, createTimeout = 10.seconds, validation = ValidationQuery.None))
                Verify.that(
                    database.modify("SET timezone = 'America/New_York'").count().toMany(), context = PoolContext(p),
                ).completesNormally(within = TEST_TIMEOUT)
                Verify.that(
                    database.query("SHOW timezone").single().map { it.get<String>("TimeZone") }, context = PoolContext(p),
                ).assertNext { assertEquals("America/New_York", it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(p.close()).completesNormally()
            },

            // --- Deferred/Phase 6 stubs ---

            dynamicTest("SCRAM-SHA-256-PLUS succeeds with channel binding") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 5 deferred — SCRAM-SHA-256-PLUS not yet implemented")
            },
            dynamicTest("password supplier called on each new connection") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — credential rotation not yet implemented")
            },
            dynamicTest("PRIMARY target connects to writable node") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — multi-host not yet implemented")
            },
            dynamicTest("cancelling a long-running query sends CancelRequest and unblocks") {
                val database = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                ))
                val p = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 1,
                    acquireTimeout = 5.seconds, createTimeout = 10.seconds, validation = ValidationQuery.None))
                val slowQuery        = database.query("SELECT pg_sleep(10)").single().toMany()
                val cancelAfterDelay = Many.items(Unit)
                    .delaySubscription(300.milliseconds)
                    .flatMapNone { p.acquiredConnections().first().cancel() }
                    .then { Many.empty<Row>() }
                Verify.that(
                    merge(slowQuery, cancelAfterDelay), context = PoolContext(p),
                ).completesWithError(within = 5.seconds).also { error ->
                    assertIs<DatabaseException.QueryCancelled>(error)
                }
                Verify.that(p.close()).completesNormally()
            },

            dynamicTest("stop container") { postgres.stop() },
        )
    }
}
