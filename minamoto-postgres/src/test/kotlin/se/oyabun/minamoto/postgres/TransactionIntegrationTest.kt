package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Tier 2 — transaction integration tests.
 *
 * Covers explicit transaction lifecycle, isolation levels, savepoints, and
 * error recovery. Requires the transaction API (Phase 2) to be implemented.
 */
@Disabled("Requires transaction API — Phase 2")
class TransactionIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )
    }

    @TestFactory
    fun `transaction tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            // --- Explicit lifecycle ---

            dynamicTest("beginTransaction sets TransactionStatus to InTransaction") { TODO() },
            dynamicTest("commitTransaction commits inserted row") { TODO() },
            dynamicTest("rollbackTransaction discards inserted row") { TODO() },
            dynamicTest("rollbackTransaction on aborted transaction restores IDLE state") { TODO() },
            dynamicTest("commitTransaction on aborted transaction throws and restores IDLE") { TODO() },
            dynamicTest("nested transaction with Join mode reuses active transaction") { TODO() },
            dynamicTest("nested transaction with New mode opens savepoint") { TODO() },

            // --- Auto-commit ---

            dynamicTest("autoCommit is true by default") { TODO() },
            dynamicTest("setAutoCommit false requires explicit commit") { TODO() },
            dynamicTest("setAutoCommit true after explicit transaction restores autoCommit") { TODO() },

            // --- Isolation levels ---

            dynamicTest("READ COMMITTED isolation level set and verified via SHOW") { TODO() },
            dynamicTest("REPEATABLE READ isolation level set and verified via SHOW") { TODO() },
            dynamicTest("SERIALIZABLE isolation level set and verified via SHOW") { TODO() },
            dynamicTest("isolation level restored to default after rollback") { TODO() },

            // --- Read-only / deferrable ---

            dynamicTest("READ ONLY transaction definition prevents writes") { TODO() },
            dynamicTest("DEFERRABLE transaction definition accepted by server") { TODO() },

            // --- Savepoints ---

            dynamicTest("createSavepoint and rollbackToSavepoint discards partial work") { TODO() },
            dynamicTest("releaseSavepoint removes savepoint without rolling back") { TODO() },
            dynamicTest("multiple savepoints nest correctly") { TODO() },

            // --- TransactionStatus exposure ---

            dynamicTest("isInTransaction returns false outside transaction") { TODO() },
            dynamicTest("isInTransaction returns true inside beginTransaction") { TODO() },

        ).also { postgres.stop() })
    }
}
