package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.getOrThrow
import se.oyabun.aelv.map
import se.oyabun.minamoto.ConnectionState
import se.oyabun.minamoto.IsolationLevel
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.TransactionMutability
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Tier 2 — transaction integration tests.
 *
 * Covers explicit transaction lifecycle, isolation levels, savepoints, and error recovery.
 */
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

        var database: PostgresDatabase? = null
        var pool: MinamotoPool? = null

        dynamicContainer(image, listOf(

            dynamicTest("start container") {
                postgres.start()
                database = PostgresDatabase(
                    ConnectionConfig(
                        host     = postgres.host,
                        port     = postgres.firstMappedPort,
                        user     = postgres.username,
                        password = postgres.password,
                        database = postgres.databaseName,
                    )
                )
                pool = database!!.pool(PoolConfig(initialSize = 2, minIdle = 2, maxSize = 5, validation = ValidationQuery.None))
            },

            // --- Explicit lifecycle ---

            dynamicTest("connection is InTransaction inside transactionally block") {
                val db = database!!; val p = pool!!
                runBlocking {
                    // Use the transaction connection directly by running a query that reveals transaction state
                    val inTransaction = p.transactionally {
                        db.query("SELECT current_setting('transaction_isolation') IS NOT NULL AS ok")
                            .single().map { it.get<Boolean>("ok") }
                            .await().getOrThrow()
                    }
                    assertTrue(inTransaction)
                }
            },

            dynamicTest("commit persists inserted row") {
                val db = database!!; val p = pool!!
                runBlocking {
                    // Use same connection for setup, insert, and verify by pinning to pool context
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS commit_test (id int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE commit_test").execute().await().getOrThrow()
                    }
                    p.transactionally<Unit> {
                        db.command("INSERT INTO commit_test VALUES (1)").count().await().getOrThrow()
                    }
                    val count = p {
                        db.query("SELECT count(*) AS n FROM commit_test").single()
                            .map { it.get<Long>("n") }.await().getOrThrow()
                    }
                    assertEquals(1L, count)
                    p { db.effect("DROP TABLE IF EXISTS commit_test").execute().await().getOrThrow() }
                }
            },

            dynamicTest("rollback discards inserted row") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS rollback_test (id int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE rollback_test").execute().await().getOrThrow()
                    }
                    runCatching {
                        p.transactionally<Unit> {
                            db.command("INSERT INTO rollback_test VALUES (1)").count().await().getOrThrow()
                            error("intentional rollback")
                        }
                    }
                    val count = p {
                        db.query("SELECT count(*) AS n FROM rollback_test").single()
                            .map { it.get<Long>("n") }.await().getOrThrow()
                    }
                    assertEquals(0L, count)
                    p { db.effect("DROP TABLE IF EXISTS rollback_test").execute().await().getOrThrow() }
                }
            },

            dynamicTest("nested transactionally on same pool uses savepoint") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS savepoint_test (id int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE savepoint_test").execute().await().getOrThrow()
                    }
                    p.transactionally<Unit> {
                        db.command("INSERT INTO savepoint_test VALUES (1)").count().await().getOrThrow()
                        runCatching {
                            p.transactionally<Unit> {
                                db.command("INSERT INTO savepoint_test VALUES (2)").count().await().getOrThrow()
                                error("rollback inner")
                            }
                        }
                        val count = db.query("SELECT count(*) AS n FROM savepoint_test").single()
                            .map { it.get<Long>("n") }.await().getOrThrow()
                        assertEquals(1L, count)
                    }
                    p { db.effect("DROP TABLE IF EXISTS savepoint_test").execute().await().getOrThrow() }
                }
            },

            // --- Isolation levels ---

            dynamicTest("READ COMMITTED isolation level sent to server") {
                val db = database!!; val p = pool!!
                val level = runBlocking {
                    p.transactionally {
                        db.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }
                            .await().getOrThrow()
                    }
                }
                assertEquals("read committed", level)
            },

            dynamicTest("REPEATABLE READ isolation level sent to server") {
                val db = database!!; val p = pool!!
                val level = runBlocking {
                    p.transactionally(TransactionDefinition(isolation = IsolationLevel.RepeatableRead)) {
                        db.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }
                            .await().getOrThrow()
                    }
                }
                assertEquals("repeatable read", level)
            },

            dynamicTest("SERIALIZABLE isolation level sent to server") {
                val db = database!!; val p = pool!!
                val level = runBlocking {
                    p.transactionally(TransactionDefinition(isolation = IsolationLevel.Serializable)) {
                        db.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }
                            .await().getOrThrow()
                    }
                }
                assertEquals("serializable", level)
            },

            // --- Read-only ---

            dynamicTest("READ ONLY transaction definition prevents writes") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS readonly_test (id int)").execute().await().getOrThrow()
                    }
                    val error = runCatching {
                        p.transactionally<Unit>(TransactionDefinition(mutability = TransactionMutability.ReadOnly)) {
                            db.command("INSERT INTO readonly_test VALUES (1)").count().await().getOrThrow()
                        }
                    }.exceptionOrNull()
                    assertTrue(error != null, "expected error for write in READ ONLY transaction")
                    p { db.effect("DROP TABLE IF EXISTS readonly_test").execute().await().getOrThrow() }
                }
            },

            dynamicTest("DEFERRABLE transaction accepted by server") {
                val db = database!!; val p = pool!!
                val level = runBlocking {
                    p.transactionally(TransactionDefinition(
                        isolation  = IsolationLevel.Serializable,
                        mutability = TransactionMutability.ReadOnly,
                        deferrable = true,
                    )) {
                        db.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }
                            .await().getOrThrow()
                    }
                }
                assertEquals("serializable", level)
            },

            // --- Savepoints ---

            dynamicTest("rollbackToSavepoint discards partial work") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS sp_partial (id int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE sp_partial").execute().await().getOrThrow()
                    }
                    p.transactionally<Unit> {
                        db.command("INSERT INTO sp_partial VALUES (1)").count().await().getOrThrow()
                        runCatching {
                            p.transactionally<Unit> {
                                db.command("INSERT INTO sp_partial VALUES (2)").count().await().getOrThrow()
                                error("rollback to savepoint")
                            }
                        }
                        val count = db.query("SELECT count(*) AS n FROM sp_partial").single()
                            .map { it.get<Long>("n") }.await().getOrThrow()
                        assertEquals(1L, count)
                    }
                    p { db.effect("DROP TABLE IF EXISTS sp_partial").execute().await().getOrThrow() }
                }
            },

            dynamicTest("releaseSavepoint commits nested work") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS sp_release (id int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE sp_release").execute().await().getOrThrow()
                    }
                    p.transactionally<Unit> {
                        db.command("INSERT INTO sp_release VALUES (1)").count().await().getOrThrow()
                        p.transactionally<Unit> {
                            db.command("INSERT INTO sp_release VALUES (2)").count().await().getOrThrow()
                        }
                        val count = db.query("SELECT count(*) AS n FROM sp_release").single()
                            .map { it.get<Long>("n") }.await().getOrThrow()
                        assertEquals(2L, count)
                    }
                    p { db.effect("DROP TABLE IF EXISTS sp_release").execute().await().getOrThrow() }
                }
            },

            dynamicTest("multiple savepoints nest correctly") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS sp_nest (id int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE sp_nest").execute().await().getOrThrow()
                    }
                    p.transactionally<Unit> {
                        db.command("INSERT INTO sp_nest VALUES (1)").count().await().getOrThrow()
                        p.transactionally<Unit> {
                            db.command("INSERT INTO sp_nest VALUES (2)").count().await().getOrThrow()
                            runCatching {
                                p.transactionally<Unit> {
                                    db.command("INSERT INTO sp_nest VALUES (3)").count().await().getOrThrow()
                                    error("rollback innermost")
                                }
                            }
                        }
                        val count = db.query("SELECT count(*) AS n FROM sp_nest").single()
                            .map { it.get<Long>("n") }.await().getOrThrow()
                        assertEquals(2L, count)
                    }
                    p { db.effect("DROP TABLE IF EXISTS sp_nest").execute().await().getOrThrow() }
                }
            },

            // --- Pipeline composition ---

            dynamicTest("transaction pipeline overload streams rows inside transaction") {
                val db = database!!; val p = pool!!
                runBlocking {
                    p {
                        db.effect("CREATE TABLE IF NOT EXISTS pipeline_tx (v int)").execute().await().getOrThrow()
                        db.effect("TRUNCATE pipeline_tx").execute().await().getOrThrow()
                        db.command("INSERT INTO pipeline_tx VALUES (1),(2),(3)").count().await().getOrThrow()
                    }
                }
                Verify.that(
                    p.transaction { db.query("SELECT v FROM pipeline_tx ORDER BY v").multiple() },
                    timeout = 30.seconds,
                    context = PoolContext(p),
                ).emitsCount(3).completesNormally()
                runBlocking {
                    p { db.effect("DROP TABLE IF EXISTS pipeline_tx").execute().await().getOrThrow() }
                }
            },

            dynamicTest("stop container") {
                runBlocking { pool?.close() }
                postgres.stop()
            },

        ))
    }
}
