package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Verify
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.fold
import se.oyabun.aelv.map
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class PostgresConnectionTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `connection tests across postgres versions`() = postgresImages.map { image ->
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
                pool = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 5, validation = ValidationQuery.None))
            },

            dynamicTest("connects and returns result from simple query") {
                Verify.that(
                    database.query("SELECT 1 AS n").single().map { it.get<Int>("n") },
                    context = PoolContext(pool),
                ).assertNext { assertEquals(1, it) }.completes(within = TEST_TIMEOUT)
            },

            dynamicTest("query streams multiple rows") {
                Verify.that(
                    database.query("SELECT 'hello' AS greeting UNION ALL SELECT 'world'")
                        .multiple()
                        .fold(emptyList<String>()) { acc, row -> acc + row.get<String>("greeting") },
                    context = PoolContext(pool),
                ).assertNext { assertEquals(listOf("hello", "world"), it) }.completes(within = TEST_TIMEOUT)
            },

            dynamicTest("pool validates connection with SELECT 1") {
                val dbWithValidation = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { postgres.password },
                    database = postgres.databaseName,
                ))
                val validatingPool = dbWithValidation.pool(PoolConfig(
                    initialSize = 1, minIdle = 1, maxSize = 3,
                    validation  = ValidationQuery.Local,
                ))
                Verify.that(
                    dbWithValidation.query("SELECT 42 AS n").single().map { it.get<Int>("n") },
                    context = PoolContext(validatingPool),
                ).assertNext { assertEquals(42, it) }.completes(within = TEST_TIMEOUT)
                Verify.that(validatingPool.close()).completes()
            },

            dynamicTest("bad credentials fail to connect") {
                val badDatabase = PostgresDatabase(ConnectionConfig(
                    host     = postgres.host,
                    port     = postgres.firstMappedPort,
                    user     = postgres.username,
                    password = { "wrongpassword" },
                    database = postgres.databaseName,
                ))
                val badPool = badDatabase.pool(PoolConfig(
                    initialSize    = 0,
                    minIdle        = 0,
                    maxSize        = 1,
                    acquireTimeout = 5.seconds,
                    createTimeout  = 10.seconds,
                    validation     = ValidationQuery.None,
                ))
                Verify.that(
                    badDatabase.query("SELECT 1 AS n").single().map { it.get<Int>("n") },
                    context = PoolContext(badPool),
                ).fails(within = 15.seconds)
                Verify.that(badPool.close()).completes()
            },

            dynamicTest("stop container") {
                Verify.that(pool.close()).completes()
                postgres.stop()
            },
        )
    }
}
