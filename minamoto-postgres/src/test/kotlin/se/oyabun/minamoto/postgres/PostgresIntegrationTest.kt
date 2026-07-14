package se.oyabun.minamoto.postgres

import se.oyabun.aelv.One
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.*
import se.oyabun.aelv.map
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.postgres.protocol.handshake
import se.oyabun.minamoto.postgres.query
import se.oyabun.minamoto.postgres.get
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

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

    private val host     = "localhost"
    private val port     = 15432
    private val database = "test"

    private fun connect(user: String, password: String): One<PostgresConnection> =
        One.defer {
            val transport       = NettyTransport()
            val nettyConnection = transport.connect(host, port).await().rightOrThrow()
            val connection      = PostgresConnection(
                id         = ConnectionId(System.nanoTime()),
                connection = nettyConnection,
                transport  = transport,
            )
            connection.handshake(user, password, database)
            connection
        }

    @Test
    fun `scram-sha-256 auth succeeds`() {
        Verify.that(connect("test", "testpass"), timeout = 10.seconds)
            .assertNext { assertEquals(ConnectionState.Idle, it.state) }
            .completesNormally()
    }

    @Test
    fun `md5 auth succeeds`() {
        Verify.that(connect("testmd5", "testmd5pass"), timeout = 10.seconds)
            .assertNext { assertEquals(ConnectionState.Idle, it.state) }
            .completesNormally()
    }

    @Test
    fun `trust auth succeeds`() {
        Verify.that(connect("testtrust", ""), timeout = 10.seconds)
            .assertNext { assertEquals(ConnectionState.Idle, it.state) }
            .completesNormally()
    }

    @Test
    fun `ping returns valid after connect`() {
        Verify.that(
            connect("test", "testpass").map(transform = suspend { connection: PostgresConnection ->
                connection.ping().also { connection.close() }
            }),
            timeout = 10.seconds,
        )
            .assertNext { assertIs<ValidationResult.Valid>(it) }
            .completesNormally()
    }

    @Test
    fun `simple query returns rows`() {
        Verify.that(
            connect("test", "testpass").flatMapMany { connection ->
                connection.query("SELECT 'hello' AS greeting UNION ALL SELECT 'world'").bind().multiple()
                    .map { row -> row.get<String>("greeting") }
            },
            timeout = 10.seconds,
        )
            .emitsNext("hello", "world")
            .completesNormally()
    }

    @Test
    fun `parameterised query returns correct result`() {
        Verify.that(
            connect("test", "testpass").flatMapMany { connection ->
                connection.query("SELECT :name::text AS name")
                    .bind("name" to "minamoto")
                    .multiple()
                    .map { row -> row.get<String>("name") }
            },
            timeout = 10.seconds,
        )
            .emitsNext("minamoto")
            .completesNormally()
    }

    @Test
    fun `wrong password throws authentication failed`() {
        val error = Verify.that(connect("test", "wrongpassword"), timeout = 10.seconds)
            .completesWithError()
        assertIs<MinamotoException.AuthenticationFailed>(error)
    }
}
