package se.oyabun.minamoto.postgres

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.One
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.map
import se.oyabun.aelv.netty.NettyTransport
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.toMaybe
import se.oyabun.aelv.toMany
import se.oyabun.minamoto.Binding
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.postgres.protocol.handshake
import java.math.BigDecimal
import java.time.OffsetTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class CodecIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    private fun connect(postgres: PostgreSQLContainer): One<PostgresDatabase> =
        One.defer {
            val transport           = NettyTransport()
            val nettyConnection     = transport.connect(postgres.host, postgres.firstMappedPort).await().rightOrThrow()
            val postgresConnection  = PostgresConnection(
                id         = ConnectionId(System.nanoTime()),
                connection = nettyConnection,
                transport  = transport,
            )
            postgresConnection.handshake(postgres.username, postgres.password, postgres.databaseName)
            PostgresDatabase(postgresConnection)
        }

    private inline fun <reified T : Any> scalar(
        postgres:  PostgreSQLContainer,
        label:     String,
        statement: String,
        column:    String,
        noinline assertion: (T) -> Unit,
    ) = dynamicTest(label) {
        val type = T::class
        Verify.that(
            connect(postgres).flatMapMany { database ->
                database.query(statement).single().toMaybe().toMany()
                    .map { row -> row.get(column, type) }
            },
            timeout = 30.seconds,
        )
            .assertNext { assertion(it) }
            .completesNormally()
    }

    private inline fun <reified T : Any> param(
        postgres:  PostgreSQLContainer,
        label:     String,
        statement: String,
        binding:   Binding,
        column:    String,
        noinline assertion: (T) -> Unit,
    ) = dynamicTest(label) {
        val type = T::class
        Verify.that(
            connect(postgres).flatMapMany { database ->
                database.query(statement).bind(binding).single().toMaybe().toMany()
                    .map { row -> row.get(column, type) }
            },
            timeout = 30.seconds,
        )
            .assertNext { assertion(it) }
            .completesNormally()
    }

    @TestFactory
    fun `codec integration across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            dynamicTest("start container") { postgres.start() },

            scalar<Boolean>(postgres, "bool column decoded as Boolean",
                "SELECT true::bool AS v", "v") { assertEquals(true, it) },
            scalar<Short>(postgres, "int2 column decoded as Short",
                "SELECT 32767::int2 AS v", "v") { assertEquals(Short.MAX_VALUE, it) },
            scalar<Int>(postgres, "int4 column decoded as Int",
                "SELECT 2147483647::int4 AS v", "v") { assertEquals(Int.MAX_VALUE, it) },
            scalar<Long>(postgres, "int8 column decoded as Long",
                "SELECT 9223372036854775807::int8 AS v", "v") { assertEquals(Long.MAX_VALUE, it) },
            scalar<Long>(postgres, "int4 column widened to Long",
                "SELECT 42::int4 AS v", "v") { assertEquals(42L, it) },
            scalar<Int>(postgres, "int2 column widened to Int",
                "SELECT 7::int2 AS v", "v") { assertEquals(7, it) },
            scalar<Float>(postgres, "float4 column decoded as Float",
                "SELECT 3.14::float4 AS v", "v") { assertEquals(3.14f, it, 0.001f) },
            scalar<Double>(postgres, "float8 column decoded as Double",
                "SELECT 3.141592653589793::float8 AS v", "v") { assertEquals(3.141592653589793, it, 1e-9) },
            scalar<Double>(postgres, "float4 column widened to Double",
                "SELECT 1.5::float4 AS v", "v") { assertEquals(1.5, it, 0.001) },
            scalar<BigDecimal>(postgres, "numeric column decoded as BigDecimal",
                "SELECT 123.45::numeric AS v", "v") { assertEquals(BigDecimal("123.45"), it) },
            scalar<String>(postgres, "text column decoded as String",
                "SELECT 'hello'::text AS v", "v") { assertEquals("hello", it) },
            scalar<String>(postgres, "varchar column decoded as String",
                "SELECT 'world'::varchar(10) AS v", "v") { assertEquals("world", it) },
            scalar<String>(postgres, "bpchar column decoded as String",
                "SELECT 'x'::bpchar AS v", "v") { assertEquals("x", it.trim()) },
            scalar<ByteArray>(postgres, "bytea column decoded as ByteArray",
                "SELECT '\\xDEADBEEF'::bytea AS v", "v") {
                assertEquals(
                    listOf<Byte>(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte()),
                    it.toList(),
                )
            },
            scalar<UUID>(postgres, "uuid column decoded as UUID",
                "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS v", "v") {
                assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), it)
            },
            scalar<LocalDate>(postgres, "date column decoded as LocalDate",
                "SELECT '2026-07-14'::date AS v", "v") { assertEquals(LocalDate(2026, 7, 14), it) },
            scalar<LocalTime>(postgres, "time column decoded as LocalTime",
                "SELECT '10:30:00'::time AS v", "v") { assertEquals(LocalTime(10, 30, 0), it) },
            scalar<OffsetTime>(postgres, "timetz column decoded as OffsetTime",
                "SELECT '14:30:00+02:00'::timetz AS v", "v") {
                assertEquals(OffsetTime.of(14, 30, 0, 0, ZoneOffset.ofHours(2)), it)
            },
            scalar<LocalDateTime>(postgres, "timestamp column decoded as LocalDateTime",
                "SELECT '2026-07-14T10:00:00'::timestamp AS v", "v") {
                assertEquals(LocalDateTime(2026, 7, 14, 10, 0, 0), it)
            },
            scalar<Instant>(postgres, "timestamptz column decoded as Instant",
                "SELECT '2026-07-14T10:00:00Z'::timestamptz AS v", "v") {
                assertEquals(Instant.parse("2026-07-14T10:00:00Z"), it)
            },
            scalar<kotlin.time.Duration>(postgres, "interval column decoded as Duration",
                "SELECT '2 hours 30 minutes'::interval AS v", "v") {
                assertEquals(2.hours + 30.minutes, it)
            },

            dynamicTest("SQL NULL with getOrNull returns null") {
                Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.query("SELECT NULL::text AS v").single().toMaybe().toMany()
                    },
                    timeout = 30.seconds,
                ).assertNext { row -> assertNull(row.getOrNull<String>("v")) }.completesNormally()
            },

            dynamicTest("SQL NULL with get throws UnexpectedNull") {
                val error = Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.query("SELECT NULL::text AS v").single().toMaybe().toMany()
                            .map { row -> row.get<String>("v") }
                    },
                    timeout = 30.seconds,
                ).completesWithError()
                assertIs<MinamotoException.UnexpectedNull>(error)
            },

            dynamicTest("non-null value with getOrNull returns value") {
                Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.query("SELECT 'hello'::text AS v").single().toMaybe().toMany()
                    },
                    timeout = 30.seconds,
                ).assertNext { row -> assertEquals("hello", row.getOrNull<String>("v")) }.completesNormally()
            },

            param<Boolean>(postgres, "Boolean parameter round-tripped",
                "SELECT :v::bool AS v", "v" to true, "v") { assertEquals(true, it) },
            param<Int>(postgres, "Int parameter round-tripped",
                "SELECT :v::int4 AS v", "v" to 42, "v") { assertEquals(42, it) },
            param<Long>(postgres, "Long parameter round-tripped",
                "SELECT :v::int8 AS v", "v" to 9999L, "v") { assertEquals(9999L, it) },
            param<Double>(postgres, "Double parameter round-tripped",
                "SELECT :v::float8 AS v", "v" to 3.14, "v") { assertEquals(3.14, it, 1e-9) },
            param<String>(postgres, "String parameter round-tripped",
                "SELECT :v::text AS v", "v" to "minamoto", "v") { assertEquals("minamoto", it) },
            param<UUID>(postgres, "UUID parameter round-tripped",
                "SELECT :v::uuid AS v",
                "v" to UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), "v") {
                assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), it)
            },
            param<LocalDate>(postgres, "LocalDate parameter round-tripped",
                "SELECT :v::date AS v", "v" to LocalDate(2026, 7, 14), "v") {
                assertEquals(LocalDate(2026, 7, 14), it)
            },
            param<LocalTime>(postgres, "LocalTime parameter round-tripped",
                "SELECT :v::time AS v", "v" to LocalTime(10, 30, 0), "v") {
                assertEquals(LocalTime(10, 30, 0), it)
            },
            param<LocalDateTime>(postgres, "LocalDateTime parameter round-tripped",
                "SELECT :v::timestamp AS v", "v" to LocalDateTime(2026, 7, 14, 10, 0, 0), "v") {
                assertEquals(LocalDateTime(2026, 7, 14, 10, 0, 0), it)
            },
            param<Instant>(postgres, "Instant parameter round-tripped",
                "SELECT :v::timestamptz AS v", "v" to Instant.parse("2026-07-14T10:00:00Z"), "v") {
                assertEquals(Instant.parse("2026-07-14T10:00:00Z"), it)
            },

            dynamicTest("INSERT returns rowsUpdated = 1") {
                Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.command("CREATE TEMP TABLE IF NOT EXISTS rows_test (id int)").count()
                            .flatMapMany {
                                database.command("INSERT INTO rows_test VALUES (1)").count()
                                    .toMaybe().toMany()
                            }
                    },
                    timeout = 30.seconds,
                ).assertNext { assertEquals(1L, it) }.completesNormally()
            },

            dynamicTest("DELETE returns rowsUpdated = 0 when no rows matched") {
                Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.command("CREATE TEMP TABLE IF NOT EXISTS empty_test (id int)").count()
                            .flatMapMany {
                                database.command("DELETE FROM empty_test WHERE id = 999").count()
                                    .toMaybe().toMany()
                            }
                    },
                    timeout = 30.seconds,
                ).assertNext { assertEquals(0L, it) }.completesNormally()
            },

            dynamicTest("server error surfaces sqlState on QueryFailed") {
                val error = Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.query("SELECT * FROM nonexistent_table_xyz").multiple()
                    },
                    timeout = 30.seconds,
                ).completesWithError()
                assertIs<MinamotoException.QueryFailed>(error)
                assertEquals("42P01", (error as MinamotoException.QueryFailed).sqlState)
            },

            dynamicTest("unique constraint violation has sqlState 23505") {
                val error = Verify.that(
                    connect(postgres).flatMapMany { database ->
                        database.command("CREATE TEMP TABLE IF NOT EXISTS uniq_test (id int PRIMARY KEY)").count()
                            .flatMapMany {
                                database.command("INSERT INTO uniq_test VALUES (1)").count()
                                    .flatMapMany {
                                        database.command("INSERT INTO uniq_test VALUES (1)").count()
                                            .toMaybe().toMany()
                                    }
                            }
                    },
                    timeout = 30.seconds,
                ).completesWithError()
                assertIs<MinamotoException.QueryFailed>(error)
                assertEquals("23505", (error as MinamotoException.QueryFailed).sqlState)
            },

            dynamicTest("stop container") { postgres.stop() },

        ))
    }
}
