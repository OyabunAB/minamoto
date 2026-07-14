package se.oyabun.minamoto.postgres

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import se.oyabun.aelv.await
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.toList
import se.oyabun.minamoto.postgres.protocol.handshake
import se.oyabun.minamoto.postgres.protocol.query
import kotlin.test.Test

class ManualConnectionTest {

    @Test
    fun `manual connect and query`() = runBlocking {
        withTimeout(30_000) {
            println("Connecting...")
            val transport  = NettyTransport()
            val connection = transport.connect("localhost", 15432).await().rightOrThrow()
            println("TCP connected: ${connection.channel}")

            val conn = PgConnection(
                id         = se.oyabun.minamoto.ConnectionId(1L),
                connection = connection,
                transport  = transport,
            )
            println("PgConnection created, handshaking...")
            conn.handshake("test", "testpass", "test")
            println("Connected! State: ${conn.state}")

            println("Pinging...")
            println("Ping: ${conn.ping()}")

            println("Querying...")
            conn.query("SELECT 'hello' AS greeting")
                .toList().await().rightOrThrow()
                .forEach { row -> println("Row: ${row.values.first()?.toString(Charsets.UTF_8)}") }

            println("Done.")
            conn.close()
        }
    }
}
