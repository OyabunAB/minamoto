package se.oyabun.minamoto.bench

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.await
import se.oyabun.aelv.rightOrThrow
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.slf4j.LoggerFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.postgres.ConnectionConfig
import se.oyabun.minamoto.postgres.PostgresDatabase
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery

/**
 * Shared benchmark state: one PostgreSQL container + one pool per driver,
 * created once per JMH trial (i.e. per benchmark class run).
 *
 * Drivers under comparison:
 *  - **minamoto** — this project's native driver
 *  - **r2dbc-postgresql** — the reference Spring/Reactor R2DBC implementation
 *  - **JDBC + HikariCP** — the traditional blocking baseline
 */
@State(Scope.Benchmark)
open class BenchmarkState {

    lateinit var postgres: PostgreSQLContainer

    lateinit var minamotoDatabase: PostgresDatabase
    lateinit var minamotoPool:     se.oyabun.minamoto.pool.MinamotoPool

    lateinit var r2dbcPool: ConnectionPool

    lateinit var hikari: HikariDataSource

    @Setup(Level.Trial)
    fun setup() {
        silenceLogging()

        postgres = PostgreSQLContainer(
            DockerImageName.parse("postgres:17-alpine").asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("bench").withUsername("bench").withPassword("bench")
        postgres.start()

        setupMinamoto()
        setupR2dbc()
        setupJdbc()
        setupSchema()
    }

    @TearDown(Level.Trial)
    fun teardown() {
        runCatching { runBlocking { minamotoPool.close().await().rightOrThrow() } }
        runCatching { runBlocking { minamotoDatabase.close().await().rightOrThrow() } }
        runCatching { r2dbcPool.dispose() }
        runCatching { hikari.close() }
        runCatching { postgres.stop() }
    }

    val poolContext get() = PoolContext(minamotoPool)

    // -------------------------------------------------------------------------

    private fun silenceLogging() {
        listOf(
            org.slf4j.Logger.ROOT_LOGGER_NAME,
            "se.oyabun",
            "io.r2dbc",
            "io.netty",
            "reactor",
            "com.zaxxer.hikari",
            "org.testcontainers",
            "com.github.dockerjava",
            "tc",
        ).forEach { name ->
            val logger = LoggerFactory.getLogger(name)
            if (logger is ch.qos.logback.classic.Logger)
                logger.level = ch.qos.logback.classic.Level.WARN
        }
    }

    private fun setupMinamoto() {
        minamotoDatabase = PostgresDatabase(ConnectionConfig(
            host     = postgres.host,
            port     = postgres.firstMappedPort,
            user     = postgres.username,
            password = { postgres.password },
            database = postgres.databaseName,
        ))
        minamotoPool = minamotoDatabase.pool(PoolConfig(
            initialSize = 10,
            minIdle     = 10,
            maxSize     = 10,
            validation  = ValidationQuery.None,
        ))
    }

    private fun setupR2dbc() {
        val factory = PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host(postgres.host)
                .port(postgres.firstMappedPort)
                .username(postgres.username)
                .password(postgres.password)
                .database(postgres.databaseName)
                .build()
        )
        r2dbcPool = ConnectionPool(
            ConnectionPoolConfiguration.builder(factory)
                .initialSize(10)
                .maxSize(10)
                .build()
        )
        r2dbcPool.warmup().block()
    }

    private fun setupJdbc() {
        hikari = HikariDataSource(HikariConfig().apply {
            jdbcUrl         = postgres.jdbcUrl
            username        = postgres.username
            password        = postgres.password
            maximumPoolSize = 10
            minimumIdle     = 10
        })
    }

    private fun setupSchema() {
        hikari.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS bench_row (
                        id      BIGSERIAL PRIMARY KEY,
                        name    TEXT        NOT NULL,
                        value   INT         NOT NULL,
                        created TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """.trimIndent())
                stmt.execute("""
                    INSERT INTO bench_row (name, value)
                    SELECT 'seed-' || i, i
                    FROM generate_series(1, 1000) AS i
                    ON CONFLICT DO NOTHING
                """.trimIndent())
            }
        }
    }
}
