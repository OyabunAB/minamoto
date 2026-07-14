package se.oyabun.minamoto.postgres

import se.oyabun.aelv.One
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.*
import se.oyabun.aelv.map
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.postgres.protocol.handshake
import se.oyabun.minamoto.postgres.query
import se.oyabun.minamoto.postgres.get
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds

class ManualConnectionTest {

    @Test
    fun `manual connect and query`() {
        Verify.that(
            One.defer {
                val transport       = NettyTransport()
                val nettyConnection = transport.connect("localhost", 15432).await().rightOrThrow()
                val connection      = PostgresConnection(
                    id         = se.oyabun.minamoto.ConnectionId(1L),
                    connection = nettyConnection,
                    transport  = transport,
                )
                connection.handshake("test", "testpass", "test")
                connection
            }.flatMapMany { connection ->
                connection.query("SELECT 'hello' AS greeting").bind().multiple()
                    .map { row -> row.get<String>("greeting") }
            },
            timeout = 30.seconds,
        )
            .emitsNext("hello")
            .completesNormally()
    }
}
