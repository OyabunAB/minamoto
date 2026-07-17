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
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
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
