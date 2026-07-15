package se.oyabun.minamoto.postgres

import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import se.oyabun.aelv.await
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig
import se.oyabun.minamoto.pool.ValidationQuery
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class ArrayAndJsonCodecIntegrationTest {

    companion object {
        val postgresImages = listOf(
            "postgres:13-alpine@sha256:fb9065b6e3e213bdc07edd372a5b2a26245840b7fb65d1fd8b6700106d51805c",
            "postgres:15-alpine@sha256:fceb6f86328c36f2438fae3b851b0cc57c4a7e69a58c866d9ce24281f2cf0c9c",
            "postgres:17-alpine@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609",
            "postgres:18beta2-alpine@sha256:0164ef2cdce5fc6136d7de2cf9864bee88f593283608facace1e6460ba63ad0c",
        )

        @Serializable
        data class Payload(val id: Int, val name: String)
    }

    private fun connectWithPool(
        postgres: PostgreSQLContainer,
        registry: CodecRegistry = CodecRegistry(),
    ): Pair<PostgresDatabase, MinamotoPool> {
        val database = PostgresDatabase(
            config   = ConnectionConfig(
                host     = postgres.host,
                port     = postgres.firstMappedPort,
                user     = postgres.username,
                password = postgres.password,
                database = postgres.databaseName,
            ),
            registry = registry,
        )
        val pool = database.pool(PoolConfig(initialSize = 1, minIdle = 1, maxSize = 5, validation = ValidationQuery.None))
        return Pair(database, pool)
    }

    private inline fun <reified T : Any> array(
        postgres:  PostgreSQLContainer,
        label:     String,
        statement: String,
        column:    String,
        noinline assertion: (T) -> Unit,
    ) = dynamicTest(label) {
        val (database, pool) = connectWithPool(postgres)
        val value = runBlocking {
            pool {
                database.query(statement).single().await().rightOrThrow().get<T>(column)
            }
        }
        assertion(value)
    }

    @TestFactory
    fun `array and JSON codec tests across postgres versions`() = postgresImages.map { image ->
        val postgres = PostgreSQLContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor("postgres")
        ).withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")

        dynamicContainer(image, listOf(

            dynamicTest("start container") { postgres.start() },

            array<List<*>>(postgres, "INT4[] column decoded as List<Int>",
                "SELECT ARRAY[1, 2, 3]::int4[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(1, 2, 3), it as List<Int>)
            },
            array<List<*>>(postgres, "INT8[] column decoded as List<Long>",
                "SELECT ARRAY[1, 2, 3]::int8[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(1L, 2L, 3L), it as List<Long>)
            },
            array<List<*>>(postgres, "FLOAT8[] column decoded as List<Double>",
                "SELECT ARRAY[1.1, 2.2, 3.3]::float8[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                val result = it as List<Double>
                assertEquals(3, result.size)
                assertEquals(1.1, result[0], 0.001)
            },
            array<List<*>>(postgres, "BOOL[] column decoded as List<Boolean>",
                "SELECT ARRAY[true, false, true]::bool[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(true, false, true), it as List<Boolean>)
            },
            array<List<*>>(postgres, "TEXT[] column decoded as List<String>",
                "SELECT ARRAY['foo', 'bar', 'baz']::text[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf("foo", "bar", "baz"), it as List<String>)
            },
            array<List<*>>(postgres, "UUID[] column decoded as List<UUID>",
                "SELECT ARRAY['550e8400-e29b-41d4-a716-446655440000']::uuid[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")), it as List<UUID>)
            },
            array<List<*>>(postgres, "TIMESTAMP[] column decoded as List<LocalDateTime>",
                "SELECT ARRAY['2026-07-14T10:00:00'::timestamp] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(LocalDateTime(2026, 7, 14, 10, 0, 0)), it as List<LocalDateTime>)
            },
            array<List<*>>(postgres, "TIMESTAMPTZ[] column decoded as List<Instant>",
                "SELECT ARRAY['2026-07-14T10:00:00Z'::timestamptz] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(Instant.parse("2026-07-14T10:00:00Z")), it as List<Instant>)
            },
            array<List<*>>(postgres, "NUMERIC[] column decoded as List<BigDecimal>",
                "SELECT ARRAY[1.5, 2.5]::numeric[] AS v", "v") {
                @Suppress("UNCHECKED_CAST")
                assertEquals(listOf(BigDecimal("1.5"), BigDecimal("2.5")), it as List<BigDecimal>)
            },

            dynamicTest("empty array round-trips as empty List") {
                val (database, pool) = connectWithPool(postgres)
                runBlocking {
                    pool {
                        val row = database.query("SELECT ARRAY[]::int4[] AS v").single().await().rightOrThrow()
                        assertEquals(emptyList<Int>(), row.get<List<*>>("v"))
                    }
                }
            },

            dynamicTest("single-element array round-trips") {
                val (database, pool) = connectWithPool(postgres)
                runBlocking {
                    pool {
                        val row = database.query("SELECT ARRAY[42]::int4[] AS v").single().await().rightOrThrow()
                        assertEquals(listOf(42), row.get<List<*>>("v"))
                    }
                }
            },

            dynamicTest("null element in array throws CodecFailed") {
                val (database, pool) = connectWithPool(postgres)
                val error = runBlocking {
                    runCatching {
                        pool {
                            val row = database.query("SELECT ARRAY[1, NULL, 3]::int4[] AS v").single().await().rightOrThrow()
                            row.get<List<*>>("v")
                        }
                    }.exceptionOrNull()
                }
                assertIs<MinamotoException.CodecFailed>(error)
            },

            dynamicTest("json column decoded as @Serializable data class") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val (database, pool) = connectWithPool(postgres, registry)
                runBlocking {
                    pool {
                        val row = database.query("SELECT '{\"id\":1,\"name\":\"walter\"}'::json AS v")
                            .single().await().rightOrThrow()
                        assertEquals(Payload(1, "walter"), row.get<Payload>("v"))
                    }
                }
            },

            dynamicTest("jsonb column decoded as @Serializable data class") {
                val registry = CodecRegistry()
                registry.registerJsonb<Payload>()
                val (database, pool) = connectWithPool(postgres, registry)
                runBlocking {
                    pool {
                        val row = database.query("SELECT '{\"id\":2,\"name\":\"jesse\"}'::jsonb AS v")
                            .single().await().rightOrThrow()
                        assertEquals(Payload(2, "jesse"), row.get<Payload>("v"))
                    }
                }
            },

            dynamicTest("@Serializable parameter encoded into json column") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val payload  = Payload(3, "skyler")
                val (database, pool) = connectWithPool(postgres, registry)
                runBlocking {
                    pool {
                        val row = database.query("SELECT :v::json AS v")
                            .bind("v" to payload)
                            .single().await().rightOrThrow()
                        assertEquals(payload, row.get<Payload>("v"))
                    }
                }
            },

            dynamicTest("@Serializable parameter encoded into jsonb column") {
                val registry = CodecRegistry()
                registry.registerJsonb<Payload>()
                val payload  = Payload(4, "hank")
                val (database, pool) = connectWithPool(postgres, registry)
                runBlocking {
                    pool {
                        val row = database.query("SELECT :v::jsonb AS v")
                            .bind("v" to payload)
                            .single().await().rightOrThrow()
                        assertEquals(payload, row.get<Payload>("v"))
                    }
                }
            },

            dynamicTest("json null column returns null via getOrNull") {
                val registry = CodecRegistry()
                registry.registerJson<Payload>()
                val (database, pool) = connectWithPool(postgres, registry)
                runBlocking {
                    pool {
                        val row = database.query("SELECT NULL::json AS v")
                            .single().await().rightOrThrow()
                        assertNull(row.getOrNull<Payload>("v"))
                    }
                }
            },

            dynamicTest("stop container") { postgres.stop() },

        ))
    }
}
