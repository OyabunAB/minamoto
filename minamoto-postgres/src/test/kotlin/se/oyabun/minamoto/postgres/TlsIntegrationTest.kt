package se.oyabun.minamoto.postgres

import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.getOrThrow
import se.oyabun.aelv.map
import se.oyabun.aelv.netty.SslMode
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.PoolContext
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * TLS integration tests.
 *
 * The SSL container builds a custom image that generates a self-signed certificate
 * and enables ssl = on in postgresql.conf via an init script.
 */
class TlsIntegrationTest {

    companion object {
        private const val PG_IMAGE = "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609"

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

    @Suppress("UNCHECKED_CAST")
    private fun sslPostgresContainer(): PostgreSQLContainer<*> {
        val image = ImageFromDockerfile()
            .withFileFromString("Dockerfile", sslDockerfile)
            .withFileFromString("ssl-init.sh", sslInitScript)
        // PostgreSQLContainer has an internal constructor that accepts Future<String> — use the string overload
        // by waiting for the image name to resolve.
        val imageName = image.get() // blocks until image is built
        return PostgreSQLContainer(DockerImageName.parse(imageName).asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass") as PostgreSQLContainer<*>
    }

    private fun connectWithPool(
        postgres: PostgreSQLContainer<*>,
        sslMode:  SslMode = SslMode.Prefer,
    ): Pair<PostgresDatabase, MinamotoPool> {
        val database = PostgresDatabase(
            ConnectionConfig(
                host     = postgres.host,
                port     = postgres.firstMappedPort,
                user     = postgres.username,
                password = postgres.password,
                database = postgres.databaseName,
                sslMode  = sslMode,
            )
        )
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

        dynamicContainer("SSL-enabled PostgreSQL", run {
            val postgres = sslPostgresContainer()
            listOf(
                dynamicTest("start SSL container") { postgres.start() },

                dynamicTest("SslMode.Disable connects plaintext to SSL-enabled server") {
                    val (db, pool) = connectWithPool(postgres, SslMode.Disable)
                    Verify.that(
                        db.query("SELECT 1 AS n").single().map { it.get<Int>("n") },
                        timeout = 30.seconds,
                        context = PoolContext(pool),
                    ).assertNext { assertEquals(1, it) }.completesNormally()
                    runBlocking { pool.close() }
                },

                dynamicTest("SslMode.Require connects and pg_stat_ssl confirms SSL") {
                    val (db, pool) = connectWithPool(postgres, SslMode.Require)
                    val usingSsl = runBlocking {
                        pool {
                            db.query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                                .single().map { it.get<Boolean>("ssl") }
                                .await().getOrThrow()
                        }
                    }
                    assertTrue(usingSsl, "expected SSL but pg_stat_ssl reports ssl=false")
                    runBlocking { pool.close() }
                },

                dynamicTest("SslMode.Prefer connects with TLS when server supports it") {
                    val (db, pool) = connectWithPool(postgres, SslMode.Prefer)
                    val usingSsl = runBlocking {
                        pool {
                            db.query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                                .single().map { it.get<Boolean>("ssl") }
                                .await().getOrThrow()
                        }
                    }
                    assertTrue(usingSsl, "SslMode.Prefer should use TLS when server supports it")
                    runBlocking { pool.close() }
                },

                dynamicTest("SslMode.Verify fails with self-signed cert and no trust store") {
                    val (_, pool) = connectWithPool(postgres, SslMode.Verify())
                    val result = runBlocking { runCatching { pool.acquire() } }
                    val failed = result.exceptionOrNull() != null ||
                        result.getOrNull() is se.oyabun.minamoto.ConnectionAcquireResult.TimedOut
                    assertTrue(failed, "expected certificate verification failure but connection succeeded")
                    runBlocking { pool.close() }
                },

                dynamicTest("stop SSL container") { postgres.stop() },
            )
        }),

        dynamicContainer("SSL fallback — plain server", run {
            val postgres = PostgreSQLContainer(
                DockerImageName.parse(PG_IMAGE).asCompatibleSubstituteFor("postgres")
            ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

            listOf(
                dynamicTest("start plain container") { postgres.start() },

                dynamicTest("SslMode.Prefer falls back to plain when server has no SSL") {
                    val (db, pool) = connectWithPool(postgres, SslMode.Prefer)
                    Verify.that(
                        db.query("SELECT 1 AS n").single().map { it.get<Int>("n") },
                        timeout = 30.seconds,
                        context = PoolContext(pool),
                    ).assertNext { assertEquals(1, it) }.completesNormally()
                    runBlocking { pool.close() }
                },

                dynamicTest("SslMode.Require throws when server has no SSL") {
                    val (_, pool) = connectWithPool(postgres, SslMode.Require)
                    val result = runBlocking { runCatching { pool.acquire() } }
                    val failed = result.exceptionOrNull() != null ||
                        result.getOrNull() is se.oyabun.minamoto.ConnectionAcquireResult.TimedOut
                    assertTrue(failed, "expected TlsFailed but connection succeeded")
                    runBlocking { pool.close() }
                },

                dynamicTest("stop plain container") { postgres.stop() },
            )
        }),
    )
}
