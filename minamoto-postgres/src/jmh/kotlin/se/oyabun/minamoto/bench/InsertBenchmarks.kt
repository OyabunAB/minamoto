package se.oyabun.minamoto.bench

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Warmup
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import se.oyabun.aelv.Verify
import se.oyabun.aelv.map
import se.oyabun.aelv.take
import se.oyabun.minamoto.postgres.get
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

/**
 * INSERT benchmarks across three drivers.
 *
 * Two scenarios:
 *  1. **insertRow** — single `INSERT … RETURNING id`
 *  2. **insertBatch** — 10 rows via JDBC batch / r2dbc add() / minamoto generate_series
 *
 * Run with:
 *   `./gradlew :minamoto-postgres:jmh -Pjmh.include=InsertBenchmarks`
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
open class InsertBenchmarks {

    // -------------------------------------------------------------------------
    // Single INSERT … RETURNING id
    // -------------------------------------------------------------------------

    @Benchmark
    fun minamoto_insertRow(state: BenchmarkState): Long {
        var id = 0L
        Verify.that(
            state.minamotoDatabase
                .query("INSERT INTO bench_row(name, value) VALUES(:name, :value) RETURNING id")
                .bind("name" to "jmh", "value" to 1)
                .multiple()
                .map { it.get<Long>("id") }
                .take(1),
            context = state.poolContext,
        ).assertNext { id = it }.completes(within = 30.seconds)
        return id
    }

    @Benchmark
    fun r2dbc_insertRow(state: BenchmarkState): Long =
        Mono.usingWhen(
            state.r2dbcPool.create(),
            { conn ->
                Mono.from(
                    conn.createStatement(
                        "INSERT INTO bench_row(name, value) VALUES($1, $2) RETURNING id"
                    ).bind("$1", "jmh").bind("$2", 1).execute()
                ).flatMap { result ->
                    Mono.from(result.map { row, _ ->
                        row.get("id", java.lang.Long::class.java)!!.toLong()
                    })
                }
            },
            { conn -> conn.close() },
        ).block()!!

    @Benchmark
    fun jdbc_insertRow(state: BenchmarkState): Long =
        state.hikari.connection.use { conn ->
            conn.prepareStatement(
                "INSERT INTO bench_row(name, value) VALUES(?, ?) RETURNING id"
            ).use { stmt ->
                stmt.setString(1, "jmh")
                stmt.setInt(2, 1)
                stmt.executeQuery().use { rs -> rs.next(); rs.getLong("id") }
            }
        }

    // -------------------------------------------------------------------------
    // Batch INSERT — 10 rows
    // -------------------------------------------------------------------------

    @Benchmark
    fun minamoto_insertBatch(state: BenchmarkState) {
        Verify.that(
            state.minamotoDatabase
                .run("INSERT INTO bench_row(name, value) SELECT 'jmh-' || i, i FROM generate_series(1,10) i")
                .execute(),
            context = state.poolContext,
        ).completes(within = 30.seconds)
    }

    @Benchmark
    fun r2dbc_insertBatch(state: BenchmarkState): Long =
        Mono.usingWhen(
            state.r2dbcPool.create(),
            { conn ->
                val stmt = conn.createStatement(
                    "INSERT INTO bench_row(name, value) VALUES($1, $2)"
                )
                (1..10).forEach { i ->
                    stmt.bind("$1", "jmh-$i").bind("$2", i)
                    if (i < 10) stmt.add()
                }
                Flux.from(stmt.execute())
                    .flatMap { result -> Mono.from(result.rowsUpdated) }
                    .reduce(0L, Long::plus)
            },
            { conn -> conn.close() },
        ).block()!!

    @Benchmark
    fun jdbc_insertBatch(state: BenchmarkState): Int =
        state.hikari.connection.use { conn ->
            conn.prepareStatement("INSERT INTO bench_row(name, value) VALUES(?, ?)").use { stmt ->
                repeat(10) { i ->
                    stmt.setString(1, "jmh-$i")
                    stmt.setInt(2, i)
                    stmt.addBatch()
                }
                stmt.executeBatch().sum()
            }
        }
}
