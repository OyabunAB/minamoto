package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Verify
import se.oyabun.aelv.map
import se.oyabun.aelv.netty.SslMode
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class TlsIntegrationTest {

    companion object {
        private const val PG_IMAGE = "postgres:17-alpine"

        private val sslDockerfile = """
            FROM $PG_IMAGE
            RUN apk add --no-cache openssl
            RUN mkdir -p /ssl && \
                openssl req -new -x509 -nodes -days 3650 \
                    -subj "/CN=localhost" \
                    -keyout /ssl/server.key \
                    -out /ssl/server.crt && \
                chmod 600 /ssl/server.key && \
                chown postgres:postgres /ssl/server.key /ssl/server.crt
            COPY ssl-init.sh /docker-entrypoint-initdb.d/ssl-init.sh
            RUN chmod +x /docker-entrypoint-initdb.d/ssl-init.sh
        """.trimIndent()

        private val sslInitScript = """
            #!/bin/sh
            cp /ssl/server.crt "${'$'}PGDATA/server.crt"
            cp /ssl/server.key "${'$'}PGDATA/server.key"
            chmod 600 "${'$'}PGDATA/server.key"
            printf '\nssl = on\nssl_cert_file = '"'"'server.crt'"'"'\nssl_key_file = '"'"'server.key'"'"'\n' >> "${'$'}PGDATA/postgresql.conf"
        """.trimIndent()
    }

    private fun sslPostgresContainer(): PostgreSQLContainer {
        val image = ImageFromDockerfile()
            .withFileFromString("Dockerfile", sslDockerfile)
            .withFileFromString("ssl-init.sh", sslInitScript)
        val imageName = image.get()
        return PostgreSQLContainer(DockerImageName.parse(imageName).asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
    }

    private fun connectWithPool(
        postgres: PostgreSQLContainer,
        sslMode:  SslMode = SslMode.Prefer,
    ): Pair<PostgresDatabase, MinamotoPool> {
        val database = PostgresDatabase(ConnectionConfig(
            host     = postgres.host,
            port     = postgres.firstMappedPort,
            user     = postgres.username,
            password = { postgres.password },
            database = postgres.databaseName,
            sslMode  = sslMode,
        ))
        val pool = database.pool(PoolConfig(
            initialSize    = 1,
            minIdle        = 1,
            maxSize        = 3,
            acquireTimeout = 5.seconds,
            createTimeout  = 10.seconds,
            validation     = ValidationQuery.None,
        ))
        return Pair(database, pool)
    }

    @TestFactory
    fun `TLS connection tests`() = listOf(

        dynamicContainer("SSL-enabled PostgreSQL", sslEnabledTests()),
        dynamicContainer("SSL fallback — plain server", plainServerTests()),
    )

    private fun sslEnabledTests(): List<DynamicNode> {
        val postgres = sslPostgresContainer()
        return listOf(
            dynamicTest("start SSL container") { postgres.start() },

            dynamicTest("SslMode.Disable connects plaintext to SSL-enabled server") {
                val (db, pool) = connectWithPool(postgres, SslMode.Disable)
                Verify.that(
                    db.query("SELECT 1 AS n").single().map { it.get<Int>("n") }, context = PoolContext(pool),
                ).assertNext { assertEquals(1, it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("SslMode.Require connects and pg_stat_ssl confirms SSL") {
                val (db, pool) = connectWithPool(postgres, SslMode.Require)
                Verify.that(
                    db.query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                        .single().map { it.get<Boolean>("ssl") }, context = PoolContext(pool),
                ).assertNext { assertTrue(it, "expected SSL but pg_stat_ssl reports ssl=false") }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("SslMode.Prefer connects with TLS when server supports it") {
                val (db, pool) = connectWithPool(postgres, SslMode.Prefer)
                Verify.that(
                    db.query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                        .single().map { it.get<Boolean>("ssl") }, context = PoolContext(pool),
                ).assertNext { assertTrue(it, "SslMode.Prefer should use TLS when server supports it") }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("SslMode.Verify fails with self-signed cert and no trust store") {
                val (db, pool) = connectWithPool(postgres, SslMode.Verify())
                Verify.that(
                    db.query("SELECT 1 AS n").single().map { it.get<Int>("n") }, context = PoolContext(pool),
                ).failed(within = 15.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("stop SSL container") { postgres.stop() },
        )
    }

    private fun plainServerTests(): List<DynamicNode> {
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(PG_IMAGE).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        return listOf(
            dynamicTest("start plain container") { postgres.start() },

            dynamicTest("SslMode.Prefer falls back to plain when server has no SSL") {
                val (db, pool) = connectWithPool(postgres, SslMode.Prefer)
                Verify.that(
                    db.query("SELECT 1 AS n").single().map { it.get<Int>("n") }, context = PoolContext(pool),
                ).assertNext { assertEquals(1, it) }.completesNormally(within = TEST_TIMEOUT)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("SslMode.Require throws when server has no SSL") {
                val (db, pool) = connectWithPool(postgres, SslMode.Require)
                Verify.that(
                    db.query("SELECT 1 AS n").single().map { it.get<Int>("n") }, context = PoolContext(pool),
                ).failed(within = 15.seconds)
                Verify.that(pool.close()).completesNormally()
            },

            dynamicTest("stop plain container") { postgres.stop() },
        )
    }
}
