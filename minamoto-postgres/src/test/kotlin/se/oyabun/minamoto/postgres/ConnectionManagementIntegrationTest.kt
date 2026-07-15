package se.oyabun.minamoto.postgres

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.await
import se.oyabun.aelv.getOrThrow
import se.oyabun.aelv.map
import se.oyabun.minamoto.pool.AcquireResult
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Tier 5 — connection management integration tests.
 *
 * Covers session parameters and ParameterStatus surfacing introduced in Phase 5.
 * TLS is covered by TlsIntegrationTest. Multi-host failover, credential rotation,
 * and CancelRequest are deferred to Phase 6.
 */
class ConnectionManagementIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )

        private val poolConfig = PoolConfig(
            initialSize    = 1,
            minIdle        = 1,
            maxSize        = 3,
            acquireTimeout = 5.seconds,
            createTimeout  = 10.seconds,
            validation     = ValidationQuery.None,
        )
    }

    @TestFactory
    fun `connection management tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            dynamicTest("start container") { postgres.start() },

            // --- Session parameters (Phase 5) ---

            dynamicTest("search_path set in StartupMessage is active on connect") {
                val database = PostgresDatabase(
                    ConnectionConfig(
                        host       = postgres.host,
                        port       = postgres.firstMappedPort,
                        user       = postgres.username,
                        password   = postgres.password,
                        database   = postgres.databaseName,
                        searchPath = listOf("myschema", "public"),
                    )
                )
                val pool = database.pool(poolConfig)
                val searchPath = runBlocking {
                    pool {
                        database.query("SHOW search_path")
                            .single()
                            .map { it.get<String>("search_path") }
                            .await().getOrThrow()
                    }
                }
                assertTrue(
                    searchPath.contains("myschema"),
                    "expected search_path to contain myschema but was: $searchPath",
                )
                runBlocking { pool.close() }
            },

            dynamicTest("timezone set in StartupMessage is active on connect") {
                val database = PostgresDatabase(
                    ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = postgres.password,
                        database = postgres.databaseName,
                        timezone = "UTC",
                    )
                )
                val pool = database.pool(poolConfig)
                val timezone = runBlocking {
                    pool {
                        database.query("SHOW timezone")
                            .single()
                            .map { it.get<String>("TimeZone") }
                            .await().getOrThrow()
                    }
                }
                assertEquals("UTC", timezone)
                runBlocking { pool.close() }
            },

            dynamicTest("applicationName appears in pg_stat_activity") {
                val database = PostgresDatabase(
                    ConnectionConfig(
                        host            = postgres.host,
                        port            = postgres.firstMappedPort,
                        user            = postgres.username,
                        password        = postgres.password,
                        database        = postgres.databaseName,
                        applicationName = "test-app",
                    )
                )
                val pool = database.pool(poolConfig)
                val applicationName = runBlocking {
                    pool {
                        database.query(
                            "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
                        ).single()
                            .map { it.get<String>("application_name") }
                            .await().getOrThrow()
                    }
                }
                assertEquals("test-app", applicationName)
                runBlocking { pool.close() }
            },

            // --- ParameterStatus (Phase 5) ---

            dynamicTest("ParameterStatus for server_version received on connect") {
                val database = PostgresDatabase(
                    ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = postgres.password,
                        database = postgres.databaseName,
                    )
                )
                val pool = database.pool(poolConfig)
                val serverVersion = runBlocking {
                    val result = pool.acquireSlot() as AcquireResult.Acquired
                    val connection = result.slot.connection as PostgresConnection
                    val version = connection.serverParameters["server_version"]
                    pool.release(connection.id)
                    version
                }
                assertNotNull(serverVersion, "server_version ParameterStatus not received during handshake")
                assertTrue(serverVersion.isNotEmpty(), "server_version was empty")
                runBlocking { pool.close() }
            },

            dynamicTest("SET timezone triggers ParameterStatus update") {
                val database = PostgresDatabase(
                    ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = postgres.password,
                        database = postgres.databaseName,
                    )
                )
                // maxSize = 1 so the same connection is reused for the SET and the subsequent inspection
                val singlePool = database.pool(poolConfig.copy(maxSize = 1))
                val timezone = runBlocking {
                    singlePool {
                        database.command("SET timezone = 'America/New_York'")
                            .count()
                            .await().getOrThrow()
                    }
                    val result = singlePool.acquireSlot() as AcquireResult.Acquired
                    val connection = result.slot.connection as PostgresConnection
                    val tz = connection.serverParameters["TimeZone"]
                    singlePool.release(connection.id)
                    tz
                }
                assertEquals("America/New_York", timezone)
                runBlocking { singlePool.close() }
            },

            // --- TLS — covered by TlsIntegrationTest ---

            // --- SCRAM-SHA-256-PLUS (Phase 5 deferred — requires channel binding) ---

            dynamicTest("SCRAM-SHA-256-PLUS succeeds with channel binding") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 5 deferred — SCRAM-SHA-256-PLUS not yet implemented")
            },

            // --- Credential rotation (Phase 6) ---

            dynamicTest("password supplier called on each new connection") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — credential rotation not yet implemented")
            },
            dynamicTest("rotated credential used for reconnect after pool eviction") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — credential rotation not yet implemented")
            },

            // --- Multi-host failover (Phase 6) ---

            dynamicTest("PRIMARY target connects to writable node") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — multi-host not yet implemented")
            },
            dynamicTest("SECONDARY target connects to read replica") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — multi-host not yet implemented")
            },
            dynamicTest("ANY target connects to first available host") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — multi-host not yet implemented")
            },
            dynamicTest("failover retries next host after connection refused") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 6 — multi-host not yet implemented")
            },

            // --- CancelRequest (Phase 5) ---

            dynamicTest("cancelling a long-running query sends CancelRequest and unblocks") {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Phase 5 — CancelRequest integration test not yet implemented")
            },

            dynamicTest("stop container") { postgres.stop() },
        ))
    }
}
