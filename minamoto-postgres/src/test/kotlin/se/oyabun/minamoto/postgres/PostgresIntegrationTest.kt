package se.oyabun.minamoto.postgres

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
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

/**
 * Integration tests against a local PostgreSQL instance.
 *
 * Requires a running postgres at localhost:15432 with the following setup:
 *
 *   -- SCRAM-SHA-256 user (default for pg 14+)
 *   CREATE USER test WITH PASSWORD 'testpass';
 *
 *   -- MD5 user
 *   SET password_encryption = 'md5';
 *   CREATE USER testmd5 WITH PASSWORD 'testmd5pass';
 *
 *   -- Trust user (no password)
 *   CREATE USER testtrust;
 *
 *   -- pg_hba.conf entries:
 *   host test testtrust all trust
 *   host test testmd5   all md5
 *   host test test      all scram-sha-256
 */
class PostgresIntegrationTest {

    private val host = "localhost"
    private val port = 15432
    private val db   = "test"

    private suspend fun connect(user: String, password: String): PgConnection {
        val transport  = NettyTransport()
        val connection = transport.connect(host, port).await().rightOrThrow()
        val conn = PgConnection(
            id         = se.oyabun.minamoto.ConnectionId(System.nanoTime()),
            connection = connection,
            transport  = transport,
        )
        conn.handshake(user, password, db)
        return conn
    }

    @Test
    fun `scram-sha-256 auth succeeds`() = runBlocking {
        withTimeout(10_000) {
            val conn = connect("test", "testpass")
            assertEquals(ConnectionState.Idle, conn.state)
            conn.close()
        }
    }

    @Test
    fun `md5 auth succeeds`() = runBlocking {
        withTimeout(10_000) {
            val conn = connect("testmd5", "testmd5pass")
            assertEquals(ConnectionState.Idle, conn.state)
            conn.close()
        }
    }

    @Test
    fun `trust auth succeeds`() = runBlocking {
        withTimeout(10_000) {
            val conn = connect("testtrust", "")
            assertEquals(ConnectionState.Idle, conn.state)
            conn.close()
        }
    }

    @Test
    fun `ping returns valid after connect`() = runBlocking {
        withTimeout(10_000) {
            val conn = connect("test", "testpass")
            assertIs<ValidationResult.Valid>(conn.ping())
            conn.close()
        }
    }

    @Test
    fun `simple query returns rows`() = runBlocking {
        withTimeout(10_000) {
            val conn = connect("test", "testpass")
            val rows = mutableListOf<String>()

            conn.query("SELECT 'hello' AS greeting UNION ALL SELECT 'world'")
                .toList().await().rightOrThrow()
                .forEach { row -> rows += row.values.first()!!.toString(Charsets.UTF_8) }

            assertEquals(listOf("hello", "world"), rows)
            conn.close()
        }
    }

    @Test
    fun `parameterised query returns correct result`() = runBlocking {
        withTimeout(10_000) {
            val conn  = connect("test", "testpass")
            val rows  = mutableListOf<String>()
            val param = "minamoto".toByteArray(Charsets.UTF_8)

            conn.query("SELECT \$1::text AS name", listOf(param))
                .toList().await().rightOrThrow()
                .forEach { row -> rows += row.values.first()!!.toString(Charsets.UTF_8) }

            assertEquals(listOf("minamoto"), rows)
            conn.close()
        }
    }

    @Test
    fun `wrong password throws authentication failed`() = runBlocking {
        withTimeout(10_000) {
            try {
                connect("test", "wrongpassword")
                error("expected authentication failure")
            } catch (e: se.oyabun.minamoto.MinamotoException.AuthenticationFailed) {
                // expected
            }
        }
    }
}
