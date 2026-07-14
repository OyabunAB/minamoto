package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.One
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.*
import se.oyabun.aelv.map
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.ValidationResult
import se.oyabun.minamoto.postgres.protocol.handshake
import se.oyabun.minamoto.postgres.query
import se.oyabun.minamoto.postgres.get
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

class PostgresConnectionTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    private fun connect(postgres: PostgreSQLContainer): One<PostgresConnection> =
        One.defer {
            val transport       = NettyTransport()
            val nettyConnection = transport.connect(postgres.host, postgres.firstMappedPort).await().rightOrThrow()
            val connection      = PostgresConnection(
                id         = ConnectionId(System.nanoTime()),
                connection = nettyConnection,
                transport  = transport,
            )
            connection.handshake(postgres.username, postgres.password, postgres.databaseName)
            connection
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
                Verify.that(connect(postgres), timeout = 30.seconds)
                    .assertNext { assertEquals(ConnectionState.Idle, it.state) }
                    .completesNormally()
            },

            dynamicTest("ping returns valid") {
                Verify.that(
                    connect(postgres).map(transform = suspend { connection: PostgresConnection ->
                        connection.ping().also { connection.close() }
                    }),
                    timeout = 30.seconds,
                )
                    .assertNext { assertIs<ValidationResult.Valid>(it) }
                    .completesNormally()
            },

            dynamicTest("query streams rows") {
                Verify.that(
                    connect(postgres).flatMapMany { connection ->
                        connection.query("SELECT 'hello' AS greeting UNION ALL SELECT 'world'").bind().multiple()
                            .map { row -> row.get<String>("greeting") }
                    },
                    timeout = 30.seconds,
                )
                    .emitsNext("hello", "world")
                    .completesNormally()
                postgres.stop()
            },
        ))
    }
}
