package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.None
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.Verify
import se.oyabun.aelv.take
import se.oyabun.aelv.then
import se.oyabun.minamoto.NotificationChannel
import se.oyabun.minamoto.NotificationSerializer
import se.oyabun.minamoto.PauseBehavior
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ListenNotifyIntegrationTest {

    companion object {
        object PingChannel : NotificationChannel<String> {
            override val name = "minamoto_ping"
        }

        object StringSerializer : NotificationSerializer<String> {
            override fun encode(value: String) = value.toByteArray(Charsets.UTF_8)
            override fun decode(bytes: ByteArray) = bytes.toString(Charsets.UTF_8)
        }

        val postgresImages = listOf(
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `listen notify integration tests`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, tests(postgres))
    }

    private fun tests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var database: PostgresDatabase

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
            },

            dynamicTest("LISTEN then NOTIFY delivers notification on channel") {
                val pool     = database.pool(PoolConfig(initialSize = 2, minIdle = 1, maxSize = 5,
                    validation = ValidationQuery.None))
                val sink     = Sinks.unicast<String>()
                val notifier = database.notifier(PingChannel, StringSerializer, pool)
                val listener = database.listener(PingChannel, StringSerializer, pool) { value ->
                    None.defer { sink.emit(value) }
                }

                listener.start()

                Verify.that(
                    notifier.notify("hello")
                        .delaySubscription(200.milliseconds)
                        .then { sink.asMany().take(1) },
                    context = PoolContext(pool),
                ).assertNext { assertEquals("hello", it) }
                 .completesNormally(within = 5.seconds)

                listener.stop()
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("notification payload is delivered correctly") {
                val pool     = database.pool(PoolConfig(initialSize = 2, minIdle = 1, maxSize = 5,
                    validation = ValidationQuery.None))
                val sink     = Sinks.unicast<String>()
                val notifier = database.notifier(PingChannel, StringSerializer, pool)
                val listener = database.listener(PingChannel, StringSerializer, pool) { value ->
                    None.defer { sink.emit(value) }
                }

                listener.start()

                Verify.that(
                    notifier.notify("alpha")
                        .delaySubscription(200.milliseconds)
                        .then { notifier.notify("beta") }
                        .then { notifier.notify("gamma") }
                        .then { sink.asMany().take(3) },
                    context = PoolContext(pool),
                ).emitsCount(3).completesNormally(within = 5.seconds)

                listener.stop()
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("Buffer behavior queues notifications while paused and delivers them on resume") {
                val pool     = database.pool(PoolConfig(initialSize = 2, minIdle = 1, maxSize = 5,
                    validation = ValidationQuery.None))
                val sink     = Sinks.unicast<String>()
                val notifier = database.notifier(PingChannel, StringSerializer, pool)
                // Default behavior is Buffer — all three notifications must arrive in order.
                val listener = database.listener(PingChannel, StringSerializer, pool) { value ->
                    None.defer { sink.emit(value) }
                }

                listener.start()

                // Start paused so "one" and "two" are sent while the listener is
                // already frozen — no timing ambiguity about which items are buffered.
                Verify.that(
                    None.defer<Unit> { listener.pause() }
                        .delaySubscription(200.milliseconds)
                        .then { notifier.notify("one") }
                        .then { notifier.notify("two") }
                        .then { None.defer<Unit> { listener.resume() } }
                        .then { notifier.notify("three") }
                        .then { sink.asMany().take(3) },
                    context = PoolContext(pool),
                ).emitsNext("one", "two", "three")
                 .completesNormally(within = 10.seconds)

                listener.stop()
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("Discard behavior drops notifications while paused") {
                val pool     = database.pool(PoolConfig(initialSize = 2, minIdle = 1, maxSize = 5,
                    validation = ValidationQuery.None))
                val sink     = Sinks.unicast<String>()
                val notifier = database.notifier(PingChannel, StringSerializer, pool)
                val listener = database.listener(PingChannel, StringSerializer, pool, PauseBehavior.Discard) { value ->
                    None.defer { sink.emit(value) }
                }

                listener.start()

                // Start paused so there is no timing ambiguity — anything sent before
                // resume() fires while the listener is already in Discard mode.
                Verify.that(
                    None.defer<Unit> { listener.pause() }
                        .delaySubscription(200.milliseconds)
                        .then { notifier.notify("dropped") }
                        .then { None.defer<Unit> { listener.resume() } }
                        .then { notifier.notify("received") }
                        .then { sink.asMany().take(1) },
                    context = PoolContext(pool),
                ).assertNext { assertEquals("received", it) }
                 .completesNormally(within = 10.seconds)

                listener.stop()
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("stop container") { postgres.stop() },
        )
    }
}
