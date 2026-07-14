package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Tier 3 — array and JSON codec integration tests.
 *
 * Verifies 1D array round-trips, null element rejection, empty arrays, and
 * JSON/JSONB with kotlinx.serialization. Requires PgRow + codec wiring (Phase 1)
 * and the CodecRegistrar SPI path.
 */
@Disabled("Requires PgRow + Bind format-code wiring — Phase 1")
class ArrayAndJsonCodecIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    @TestFactory
    fun `array and JSON codec tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            // --- 1D array decode ---

            dynamicTest("INT4[] column decoded as List<Int>") { TODO() },
            dynamicTest("INT8[] column decoded as List<Long>") { TODO() },
            dynamicTest("FLOAT8[] column decoded as List<Double>") { TODO() },
            dynamicTest("BOOL[] column decoded as List<Boolean>") { TODO() },
            dynamicTest("TEXT[] column decoded as List<String>") { TODO() },
            dynamicTest("UUID[] column decoded as List<UUID>") { TODO() },
            dynamicTest("TIMESTAMP[] column decoded as List<LocalDateTime>") { TODO() },
            dynamicTest("TIMESTAMPTZ[] column decoded as List<Instant>") { TODO() },
            dynamicTest("NUMERIC[] column decoded as List<BigDecimal>") { TODO() },
            dynamicTest("BYTEA[] column decoded as List<ByteArray>") { TODO() },

            // --- Array parameter encoding ---

            dynamicTest("List<Int> parameter encoded as INT4[]") { TODO() },
            dynamicTest("List<String> parameter encoded as TEXT[]") { TODO() },
            dynamicTest("List<UUID> parameter encoded as UUID[]") { TODO() },

            // --- Edge cases ---

            dynamicTest("empty array round-trips as empty List") { TODO() },
            dynamicTest("null element in array throws CodecFailed") { TODO() },
            dynamicTest("single-element array round-trips") { TODO() },

            // --- JSON ---

            dynamicTest("json column decoded as @Serializable data class") { TODO() },
            dynamicTest("jsonb column decoded as @Serializable data class") { TODO() },
            dynamicTest("@Serializable parameter encoded into json column") { TODO() },
            dynamicTest("@Serializable parameter encoded into jsonb column") { TODO() },
            dynamicTest("json null column returns null via getOrNull") { TODO() },
            dynamicTest("jsonb preserves field order on round-trip") { TODO() },
            dynamicTest("custom Json instance with lenient mode used for decode") { TODO() },

            // --- Custom CodecRegistrar SPI ---

            dynamicTest("CodecRegistrar on classpath is discovered and registered at pool creation") { TODO() },
            dynamicTest("CodecRegistrar disabled via PoolConfig.codecDiscovery = false") { TODO() },

        ).also { postgres.stop() })
    }
}
