package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Many
import se.oyabun.aelv.Verify
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.map
import se.oyabun.aelv.then
import se.oyabun.minamoto.IsolationLevel
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.TransactionMutability
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import se.oyabun.minamoto.transactionally
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class TransactionIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine",
            "postgres:15-alpine",
            "postgres:17-alpine",
            "postgres:18beta2-alpine",
        )
    }

    @TestFactory
    fun `transaction tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        dynamicContainer(image, tests(postgres))
    }

    private fun tests(postgres: PostgreSQLContainer): List<DynamicNode> {
        lateinit var database: PostgresDatabase
        lateinit var pool: MinamotoPool
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
                pool = database.pool(PoolConfig(initialSize = 2, minIdle = 2, maxSize = 5, validation = ValidationQuery.None))
            },

            // --- Commit persists rows ---

            dynamicTest("commit persists inserted row") {
                // DDL setup outside transaction
                Verify.that(
                    database.run("CREATE TABLE IF NOT EXISTS commit_test (id int)").execute().then { database.run("DELETE FROM commit_test").execute().toMany() }, context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)

                Verify.that(
                    transactionally {
                        database.modify("INSERT INTO commit_test VALUES (1)").count().toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals(1L, it) }.completesNormally(within = TEST_TIMEOUT)

                Verify.that(
                    database.query("SELECT count(*) AS n FROM commit_test").single()
                        .map { it.get<Long>("n") }, context = PoolContext(pool),
                ).assertNext { assertEquals(1L, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            // --- Rollback discards rows ---

            dynamicTest("rollback discards inserted row") {
                Verify.that(
                    database.run("CREATE TABLE IF NOT EXISTS rollback_test (id int)").execute().then { database.run("DELETE FROM rollback_test").execute().toMany() }, context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)

                Verify.that(
                    transactionally {
                        database.modify("INSERT INTO rollback_test VALUES (1)").count()
                            .flatMapMany { Many.error<Long>(RuntimeException("intentional rollback")) }
                    }, context = PoolContext(pool),
                ).failed(within = TEST_TIMEOUT)

                Verify.that(
                    database.query("SELECT count(*) AS n FROM rollback_test").single()
                        .map { it.get<Long>("n") }, context = PoolContext(pool),
                ).assertNext { assertEquals(0L, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            // --- Savepoints ---

            dynamicTest("nested transactionally uses savepoint — inner rollback preserved outer") {
                Verify.that(
                    database.run("CREATE TABLE IF NOT EXISTS savepoint_test (id int)").execute().then { database.run("DELETE FROM savepoint_test").execute().toMany() }, context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)

                val savepointPipeline: Many<Long> = transactionally(block = fun(): Many<Long> {
                    val inner: Many<Long> = transactionally(block = fun(): Many<Long> {
                        return database.modify("INSERT INTO savepoint_test VALUES (2)").count()
                            .flatMapMany { Many.error<Long>(RuntimeException("inner fail")) }
                    })
                    return database.modify("INSERT INTO savepoint_test VALUES (1)").count()
                        .flatMapMany { inner.recover { Many.items(0L) } }
                        .flatMap {
                            database.query("SELECT count(*) AS n FROM savepoint_test")
                                .single().map { it.get<Long>("n") }.toMany()
                        }
                })
                Verify.that(
                    savepointPipeline, context = PoolContext(pool),
                ).assertNext { assertEquals(1L, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("nested savepoint released commits inner work") {
                Verify.that(
                    database.run("CREATE TABLE IF NOT EXISTS sp_release (id int)").execute().then { database.run("DELETE FROM sp_release").execute().toMany() }, context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)

                val releasePipeline: Many<Long> = transactionally(block = fun(): Many<Long> {
                    val inner: Many<Long> = transactionally(block = fun(): Many<Long> {
                        return database.modify("INSERT INTO sp_release VALUES (2)").count().toMany()
                    })
                    return database.modify("INSERT INTO sp_release VALUES (1)").count()
                        .flatMapMany { inner }
                        .flatMap {
                            database.query("SELECT count(*) AS n FROM sp_release")
                                .single().map { it.get<Long>("n") }.toMany()
                        }
                })
                Verify.that(
                    releasePipeline, context = PoolContext(pool),
                ).assertNext { assertEquals(2L, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("multiple nested savepoints compose correctly") {
                Verify.that(
                    database.run("CREATE TABLE IF NOT EXISTS sp_nest (id int)").execute().then { database.run("DELETE FROM sp_nest").execute().toMany() }, context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)

                val nestPipeline: Many<Long> = transactionally(block = fun(): Many<Long> {
                    val innermost: Many<Long> = transactionally(block = fun(): Many<Long> {
                        return database.modify("INSERT INTO sp_nest VALUES (3)").count()
                            .flatMapMany { Many.error<Long>(RuntimeException("innermost fail")) }
                    })
                    val mid: Many<Long> = transactionally(block = fun(): Many<Long> {
                        return database.modify("INSERT INTO sp_nest VALUES (2)").count()
                            .flatMapMany { innermost.recover { Many.items(0L) } }
                    })
                    return database.modify("INSERT INTO sp_nest VALUES (1)").count()
                        .flatMapMany { mid }
                        .flatMap {
                            database.query("SELECT count(*) AS n FROM sp_nest")
                                .single().map { it.get<Long>("n") }.toMany()
                        }
                })
                Verify.that(
                    nestPipeline, context = PoolContext(pool),
                ).assertNext { assertEquals(2L, it) }.completesNormally(within = TEST_TIMEOUT)
            },

            // --- Isolation levels ---

            dynamicTest("READ COMMITTED isolation sent to server") {
                Verify.that(
                    transactionally {
                        database.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }.toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals("read committed", it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("REPEATABLE READ isolation sent to server") {
                Verify.that(
                    transactionally(TransactionDefinition(isolation = IsolationLevel.RepeatableRead)) {
                        database.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }.toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals("repeatable read", it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("SERIALIZABLE isolation sent to server") {
                Verify.that(
                    transactionally(TransactionDefinition(isolation = IsolationLevel.Serializable)) {
                        database.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }.toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals("serializable", it) }.completesNormally(within = TEST_TIMEOUT)
            },

            // --- Read-only ---

            dynamicTest("READ ONLY transaction prevents writes") {
                Verify.that(
                    database.run("CREATE TABLE IF NOT EXISTS readonly_test (id int)").execute().then { database.run("DELETE FROM readonly_test").execute().toMany() }, context = PoolContext(pool),
                ).completesNormally(within = TEST_TIMEOUT)

                Verify.that(
                    transactionally(TransactionDefinition(mutability = TransactionMutability.ReadOnly)) {
                        database.modify("INSERT INTO readonly_test VALUES (1)").count().toMany()
                    }, context = PoolContext(pool),
                ).failed(within = TEST_TIMEOUT)
            },

            dynamicTest("DEFERRABLE SERIALIZABLE READ ONLY accepted by server") {
                Verify.that(
                    transactionally(TransactionDefinition(
                        isolation  = IsolationLevel.Serializable,
                        mutability = TransactionMutability.ReadOnly,
                        deferrable = true,
                    )) {
                        database.query("SHOW transaction_isolation").single()
                            .map { it.get<String>("transaction_isolation") }.toMany()
                    }, context = PoolContext(pool),
                ).assertNext { assertEquals("serializable", it) }.completesNormally(within = TEST_TIMEOUT)
            },

            dynamicTest("stop container") {
                Verify.that(pool.close()).completesNormally()
                postgres.stop()
            },
        )
    }
}
