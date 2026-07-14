package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Tier 5 — extended type coverage tests.
 *
 * Covers types that have no direct Kotlin equivalent (geometric, network, hstore,
 * enum, pgvector) and advanced codec scenarios. Each group is gated on the
 * relevant phase or extension being available in the test database.
 *
 * Geometric, hstore, pgvector require the corresponding Postgres extensions.
 * Enum types require DDL setup in the test container before assertions run.
 */
@Disabled("Requires extended codec registration — post Phase 1")
class ExtendedTypeIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    @TestFactory
    fun `extended type tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            // --- User-defined enum types ---

            dynamicTest("Kotlin enum registered for PG enum OID encodes and decodes") { TODO() },
            dynamicTest("PG enum column decoded as String without explicit codec") { TODO() },
            dynamicTest("ENUM[] decoded as List<MyEnum>") { TODO() },

            // --- Network types ---

            dynamicTest("inet column decoded as InetAddress") { TODO() },
            dynamicTest("InetAddress parameter encoded into inet column") { TODO() },
            dynamicTest("inet[] decoded as List<InetAddress>") { TODO() },

            // --- Geometric types (requires PostGIS or native PG geometric) ---

            dynamicTest("point column decoded") { TODO() },
            dynamicTest("box column decoded") { TODO() },
            dynamicTest("circle column decoded") { TODO() },
            dynamicTest("line column decoded") { TODO() },
            dynamicTest("lseg column decoded") { TODO() },
            dynamicTest("path column decoded") { TODO() },
            dynamicTest("polygon column decoded") { TODO() },

            // --- hstore (requires hstore extension) ---

            dynamicTest("hstore column decoded as Map<String, String>") { TODO() },
            dynamicTest("Map<String, String> parameter encoded into hstore column") { TODO() },
            dynamicTest("hstore with null value entry round-trips") { TODO() },

            // --- pgvector (requires vector extension) ---

            dynamicTest("vector column decoded as FloatArray") { TODO() },
            dynamicTest("FloatArray parameter encoded into vector column") { TODO() },
            dynamicTest("cosine distance query returns correct ordering") { TODO() },

            // --- 2D arrays ---

            dynamicTest("INT4[][] decoded as List<List<Int>>") { TODO() },
            dynamicTest("TEXT[][] decoded as List<List<String>>") { TODO() },
            dynamicTest("null element in 2D array throws CodecFailed") { TODO() },

            // --- Object codec (default type per OID) ---

            dynamicTest("bool column read as Any returns Boolean") { TODO() },
            dynamicTest("int4 column read as Any returns Int") { TODO() },
            dynamicTest("text column read as Any returns String") { TODO() },
            dynamicTest("uuid column read as Any returns UUID") { TODO() },
            dynamicTest("timestamptz column read as Any returns Instant") { TODO() },
            dynamicTest("json column read as Any returns String") { TODO() },

            // --- COPY IN (Phase 3+) ---

            dynamicTest("COPY FROM STDIN with binary stream inserts rows") { TODO() },
            dynamicTest("COPY row count matches inserted data") { TODO() },

        ).also { postgres.stop() })
    }
}
