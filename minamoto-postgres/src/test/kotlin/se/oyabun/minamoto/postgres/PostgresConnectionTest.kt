package se.oyabun.minamoto.postgres

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.await
import se.oyabun.aelv.fold
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.ConnectionAcquireResult
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.test.assertIs

class PostgresConnectionTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    private fun connectWithPool(postgres: PostgreSQLContainer): Pair<PostgresDatabase, se.oyabun.minamoto.pool.MinamotoPool> {
        val database = PostgresDatabase(
            ConnectionConfig(
                host     = postgres.host,
                port     = postgres.firstMappedPort,
                user     = postgres.username,
                password = postgres.password,
                database = postgres.databaseName,
            )
        )
        val pool = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 5, validation = ValidationQuery.None))
        return Pair(database, pool)
    }

    @TestFactory
    fun `connection tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        )
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")

        dynamicContainer(image, listOf(

            dynamicTest("connects and authenticates") {
                postgres.start()
                val (_, pool) = connectWithPool(postgres)
                runBlocking {
                    val result = pool.acquire()
                    assertIs<ConnectionAcquireResult.Acquired>(result)
                    val connection = (result as ConnectionAcquireResult.Acquired).connection as PostgresConnection
                    assertEquals(ConnectionState.Idle, connection.state)
                    pool.release(connection.id)
                }
            },

            dynamicTest("ping returns valid") {
                val (_, pool) = connectWithPool(postgres)
                runBlocking {
                    val result = pool.acquire()
                    assertIs<ConnectionAcquireResult.Acquired>(result)
                    val connection = (result as ConnectionAcquireResult.Acquired).connection as PostgresConnection
                    val ping = connection.ping()
                    connection.close()
                    assertIs<ValidationResult.Valid>(ping)
                }
            },

            dynamicTest("query streams rows") {
                val (database, pool) = connectWithPool(postgres)
                val results = runBlocking {
                    pool {
                        database.query("SELECT 'hello' AS greeting UNION ALL SELECT 'world'")
                            .bind().multiple()
                            .fold(emptyList<String>()) { acc: List<String>, row: Row -> acc + row.get<String>("greeting") }
                            .await().rightOrThrow()
                    }
                }
                assertEquals(listOf("hello", "world"), results)
                postgres.stop()
            },
        ))
    }
}
