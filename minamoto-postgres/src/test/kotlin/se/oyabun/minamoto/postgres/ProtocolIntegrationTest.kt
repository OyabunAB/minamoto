package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Tier 4 — protocol completeness integration tests.
 *
 * Covers LISTEN/NOTIFY, SQLSTATE → exception mapping, cursor paging under backpressure,
 * RETURNING clause, and named prepared statement cache. Each area maps to a distinct
 * phase of the roadmap.
 */
@Disabled("Requires Phase 1 (PgRow), Phase 2 (transactions), Phase 3 (statement cache), Phase 4 (notifications)")
class ProtocolIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `protocol integration tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            // --- LISTEN / NOTIFY (Phase 4) ---

            dynamicTest("LISTEN then NOTIFY delivers notification on channel") { TODO() },
            dynamicTest("notification payload is delivered correctly") { TODO() },
            dynamicTest("notification delivered with correct sender PID") { TODO() },
            dynamicTest("notification stream completes when connection is closed") { TODO() },
            dynamicTest("multiple channels can be listened to simultaneously") { TODO() },
            dynamicTest("UNLISTEN stops notification delivery for that channel") { TODO() },

            // --- SQLSTATE → exception mapping (Phase 1) ---

            dynamicTest("unique constraint violation sqlState 23505 surfaces on QueryFailed") { TODO() },
            dynamicTest("not-null violation sqlState 23502 surfaces on QueryFailed") { TODO() },
            dynamicTest("foreign key violation sqlState 23503 surfaces on QueryFailed") { TODO() },
            dynamicTest("undefined table sqlState 42P01 surfaces on QueryFailed") { TODO() },
            dynamicTest("undefined column sqlState 42703 surfaces on QueryFailed") { TODO() },
            dynamicTest("permission denied sqlState 42501 surfaces on QueryFailed") { TODO() },
            dynamicTest("syntax error sqlState 42601 surfaces on QueryFailed") { TODO() },

            // --- Cursor paging / backpressure (already partially covered) ---

            dynamicTest("large result set streams via PortalSuspended across multiple Execute rounds") { TODO() },
            dynamicTest("fetchSize 1 forces one row per Execute round-trip") { TODO() },
            dynamicTest("cancelling stream mid-flight closes portal") { TODO() },

            // --- RETURNING clause (Phase 3) ---

            dynamicTest("INSERT RETURNING delivers generated id as row") { TODO() },
            dynamicTest("UPDATE RETURNING delivers updated columns") { TODO() },
            dynamicTest("INSERT RETURNING with multiple columns delivers full row") { TODO() },

            // --- Named prepared statement cache (Phase 3) ---

            dynamicTest("same SQL executed twice reuses cached prepared statement") { TODO() },
            dynamicTest("cache eviction under bounded size forces re-prepare") { TODO() },
            dynamicTest("invalidated cached statement is transparently re-prepared") { TODO() },
            dynamicTest("disabled cache re-parses on every execution") { TODO() },

            // --- Named portals (Phase 3) ---

            dynamicTest("concurrent statements on one connection use distinct portal names") { TODO() },

            // --- ParameterStatus surfacing (Phase 5) ---

            dynamicTest("SET timezone emits ParameterStatus update") { TODO() },

            // --- NoticeResponse surfacing (Phase 5) ---

            dynamicTest("server notice is surfaced at configured log level") { TODO() },

        ).also { postgres.stop() })
    }
}
