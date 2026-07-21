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
 * Round-trip SELECT benchmarks across three drivers.
 *
 * Three scenarios:
 *  1. **constant** — `SELECT 42::int4` — minimum data, pure round-trip overhead
 *  2. **singleRow** — full row decode from a pre-populated table
 *  3. **hundredRows** — streaming decode of 100 rows
 *
 * Run with:
 *   `./gradlew :minamoto-postgres:jmh -Pjmh.include=SelectBenchmarks`
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
open class SelectBenchmarks {

    // -------------------------------------------------------------------------
    // SELECT constant — `SELECT 42::int4 AS v`
    // -------------------------------------------------------------------------

    @Benchmark
    fun minamoto_selectConstant(state: BenchmarkState): Int {
        var result = 0
        Verify.that(
            state.minamotoDatabase.query("SELECT 42::int4 AS v")
                .multiple()
                .map { it.get<Int>("v") }
                .take(1),
            context = state.poolContext,
        ).assertNext { result = it }.completes(within = 30.seconds)
        return result
    }

    @Benchmark
    fun r2dbc_selectConstant(state: BenchmarkState): Int =
        Mono.usingWhen(
            state.r2dbcPool.create(),
            { conn ->
                Mono.from(conn.createStatement("SELECT 42::int4 AS v").execute())
                    .flatMap { result ->
                        Mono.from(result.map { row, _ ->
                            row.get("v", Integer::class.java)!!.toInt()
                        })
                    }
            },
            { conn -> conn.close() },
        ).block()!!

    @Benchmark
    fun jdbc_selectConstant(state: BenchmarkState): Int =
        state.hikari.connection.use { conn ->
            conn.prepareStatement("SELECT 42::int4 AS v").use { stmt ->
                stmt.executeQuery().use { rs -> rs.next(); rs.getInt("v") }
            }
        }

    // -------------------------------------------------------------------------
    // SELECT single row by primary key
    // -------------------------------------------------------------------------

    @Benchmark
    fun minamoto_selectSingleRow(state: BenchmarkState): String {
        var result = ""
        Verify.that(
            state.minamotoDatabase.query("SELECT name, value FROM bench_row WHERE id = 1")
                .multiple()
                .map { "${it.get<String>("name")}:${it.get<Int>("value")}" }
                .take(1),
            context = state.poolContext,
        ).assertNext { result = it }.completes(within = 30.seconds)
        return result
    }

    @Benchmark
    fun r2dbc_selectSingleRow(state: BenchmarkState): String =
        Mono.usingWhen(
            state.r2dbcPool.create(),
            { conn ->
                Mono.from(
                    conn.createStatement("SELECT name, value FROM bench_row WHERE id = $1")
                        .bind("$1", 1L)
                        .execute()
                ).flatMap { result ->
                    Mono.from(result.map { row, _ ->
                        "${row.get("name", String::class.java)}:${row.get("value", Integer::class.java)}"
                    })
                }
            },
            { conn -> conn.close() },
        ).block()!!

    @Benchmark
    fun jdbc_selectSingleRow(state: BenchmarkState): String =
        state.hikari.connection.use { conn ->
            conn.prepareStatement("SELECT name, value FROM bench_row WHERE id = ?").use { stmt ->
                stmt.setLong(1, 1L)
                stmt.executeQuery().use { rs ->
                    rs.next()
                    "${rs.getString("name")}:${rs.getInt("value")}"
                }
            }
        }

    // -------------------------------------------------------------------------
    // SELECT 100 rows — streaming / batch decode
    // -------------------------------------------------------------------------

    @Benchmark
    fun minamoto_select100Rows(state: BenchmarkState): Long {
        var count = 0L
        Verify.that(
            state.minamotoDatabase.query("SELECT id, name, value FROM bench_row LIMIT 100")
                .multiple()
                .map { it.get<Long>("id") }
                .take(100),
            context = state.poolContext,
        ).emitsCount(100).completes(within = 30.seconds)
        return count
    }

    @Benchmark
    fun r2dbc_select100Rows(state: BenchmarkState): Long =
        Mono.usingWhen(
            state.r2dbcPool.create(),
            { conn ->
                Flux.from(
                    conn.createStatement("SELECT id, name, value FROM bench_row LIMIT 100").execute()
                ).flatMap { result ->
                    result.map { row, _ -> row.get("id", java.lang.Long::class.java) }
                }.count()
            },
            { conn -> conn.close() },
        ).block()!!

    @Benchmark
    fun jdbc_select100Rows(state: BenchmarkState): Int =
        state.hikari.connection.use { conn ->
            conn.prepareStatement("SELECT id, name, value FROM bench_row LIMIT 100").use { stmt ->
                stmt.executeQuery().use { rs ->
                    var count = 0
                    while (rs.next()) { rs.getLong("id"); count++ }
                    count
                }
            }
        }
}
