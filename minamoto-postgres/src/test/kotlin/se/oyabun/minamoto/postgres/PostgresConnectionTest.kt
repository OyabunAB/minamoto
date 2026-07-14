package se.oyabun.minamoto.postgres

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import se.oyabun.aelv.await
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.toList
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.postgres.protocol.handshake
import se.oyabun.minamoto.postgres.protocol.query
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

@Testcontainers
class PostgresConnectionTest {

    companion object {
        @Container
        @JvmField
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
    }

    private suspend fun connection(): PgConnection {
        val transport  = NettyTransport()
        val connection = transport.connect(postgres.host, postgres.firstMappedPort).await().rightOrThrow()
        val conn = PgConnection(
            id         = se.oyabun.minamoto.ConnectionId(System.nanoTime()),
            connection = connection,
            transport  = transport,
        )
        conn.handshake(postgres.username, postgres.password, postgres.databaseName)
        return conn
    }

    @Test
    fun `connects and authenticates`() = runBlocking {
        withTimeout(30_000) {
            val conn = connection()
            assertEquals(ConnectionState.Idle, conn.state)
            conn.close()
        }
    }

    @Test
    fun `ping returns valid`() = runBlocking {
        withTimeout(30_000) {
            val conn = connection()
            assertIs<ValidationResult.Valid>(conn.ping())
            conn.close()
        }
    }

    @Test
    fun `query streams rows`() = runBlocking {
        withTimeout(30_000) {
            val conn = connection()
            val rows = mutableListOf<String>()

            conn.query("SELECT 'hello' AS greeting UNION ALL SELECT 'world'")
                .toList().await().rightOrThrow()
                .forEach { row -> rows += row.values.first()!!.toString(Charsets.UTF_8) }

            assertEquals(listOf("hello", "world"), rows)
            conn.close()
        }
    }
}
