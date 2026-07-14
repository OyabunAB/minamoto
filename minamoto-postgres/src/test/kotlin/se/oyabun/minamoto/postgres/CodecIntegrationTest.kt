package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Tier 1 — codec integration tests.
 *
 * Verifies that every built-in type round-trips correctly through [se.oyabun.minamoto.postgres.codec.CodecRegistry]
 * when executing real queries against Postgres. Covers binary format negotiation in Bind,
 * null handling, parameter encoding, and rowsUpdated from CommandComplete.
 *
 * Requires [se.oyabun.minamoto.postgres.PgRow] and Bind format-code wiring to be implemented.
 */
@Disabled("Requires PgRow + Bind format-code wiring — Phase 1")
class CodecIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    @TestFactory
    fun `codec integration across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            // --- Scalar decode via row.get<T> ---

            dynamicTest("bool column decoded as Boolean") { TODO() },
            dynamicTest("int2 column decoded as Short") { TODO() },
            dynamicTest("int4 column decoded as Int") { TODO() },
            dynamicTest("int8 column decoded as Long") { TODO() },
            dynamicTest("int4 column widened to Long") { TODO() },
            dynamicTest("int2 column widened to Int") { TODO() },
            dynamicTest("float4 column decoded as Float") { TODO() },
            dynamicTest("float8 column decoded as Double") { TODO() },
            dynamicTest("float4 column widened to Double") { TODO() },
            dynamicTest("numeric column decoded as BigDecimal") { TODO() },
            dynamicTest("text column decoded as String") { TODO() },
            dynamicTest("varchar column decoded as String") { TODO() },
            dynamicTest("bpchar column decoded as String") { TODO() },
            dynamicTest("bytea column decoded as ByteArray") { TODO() },
            dynamicTest("uuid column decoded as UUID") { TODO() },
            dynamicTest("date column decoded as LocalDate") { TODO() },
            dynamicTest("time column decoded as LocalTime") { TODO() },
            dynamicTest("timetz column decoded as OffsetTime") { TODO() },
            dynamicTest("timestamp column decoded as LocalDateTime") { TODO() },
            dynamicTest("timestamptz column decoded as Instant") { TODO() },
            dynamicTest("interval column decoded as Duration") { TODO() },

            // --- Null handling ---

            dynamicTest("SQL NULL with getOrNull returns null") { TODO() },
            dynamicTest("SQL NULL with get throws UnexpectedNull") { TODO() },
            dynamicTest("non-null value with getOrNull returns value") { TODO() },

            // --- Parameter encoding ---

            dynamicTest("Boolean parameter encoded and round-tripped") { TODO() },
            dynamicTest("Int parameter encoded and round-tripped") { TODO() },
            dynamicTest("Long parameter encoded and round-tripped") { TODO() },
            dynamicTest("Double parameter encoded and round-tripped") { TODO() },
            dynamicTest("BigDecimal parameter encoded and round-tripped") { TODO() },
            dynamicTest("String parameter encoded and round-tripped") { TODO() },
            dynamicTest("ByteArray parameter encoded and round-tripped") { TODO() },
            dynamicTest("UUID parameter encoded and round-tripped") { TODO() },
            dynamicTest("LocalDate parameter encoded and round-tripped") { TODO() },
            dynamicTest("LocalTime parameter encoded and round-tripped") { TODO() },
            dynamicTest("LocalDateTime parameter encoded and round-tripped") { TODO() },
            dynamicTest("Instant parameter encoded and round-tripped") { TODO() },
            dynamicTest("Duration parameter encoded and round-tripped") { TODO() },

            // --- Binary format negotiation ---

            dynamicTest("binary format code 1 sent for int4 column in Bind") { TODO() },
            dynamicTest("binary format code 1 sent for uuid column in Bind") { TODO() },
            dynamicTest("text format code 0 sent for numeric column in Bind") { TODO() },

            // --- rowsUpdated ---

            dynamicTest("INSERT returns rowsUpdated = 1") { TODO() },
            dynamicTest("UPDATE affecting 3 rows returns rowsUpdated = 3") { TODO() },
            dynamicTest("DELETE returns rowsUpdated = 0 when no rows matched") { TODO() },
            dynamicTest("DDL returns rowsUpdated = 0") { TODO() },

            // --- Error mapping ---

            dynamicTest("server error response surfaces sqlState on QueryFailed") { TODO() },
            dynamicTest("unique constraint violation has sqlState 23505") { TODO() },
            dynamicTest("unknown column reference has sqlState 42703") { TODO() },

        ).also { postgres.stop() })
    }
}
