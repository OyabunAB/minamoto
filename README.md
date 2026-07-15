# minamoto

Reactive, non-blocking PostgreSQL driver for Kotlin. Built on [aelv](https://github.com/OyabunAB/aelv) and [aelv-netty](https://github.com/OyabunAB/aelv-netty). No Reactor, no RxJava.

## Requirements

- Kotlin 2.x
- JVM 21+
- PostgreSQL 13 or later
- GitHub Packages credentials (`GITHUB_ACTOR` / `GITHUB_TOKEN`)

## Modules

| Module | Purpose |
|---|---|
| `minamoto-core` | Interfaces: `Database`, `QueryBuilder`, `BoundQuery`, `CommandBuilder`, `BoundCommand`, `EffectBuilder`, `BoundEffect`, `Row`, `Connection`, `ConnectionFactory` |
| `minamoto-pool` | `MinamotoPool` connection pool with idle/lifetime eviction, validation hooks, deadlock detection |
| `minamoto-postgres` | PostgreSQL implementation: `PostgresDatabase`, `PostgresConnectionFactory`, codec layer, PGwire protocol |

## Install

```kotlin
// build.gradle.kts
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/OyabunAB/minamoto")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("se.oyabun:minamoto-core:<version>")
    implementation("se.oyabun:minamoto-pool:<version>")
    implementation("se.oyabun:minamoto-postgres:<version>")
}
```

`minamoto-core` is a transitive dependency of `minamoto-postgres` and `minamoto-pool`. Include it explicitly only if you need to code against the interfaces without the implementation.

## Quick start

```kotlin
val database = PostgresDatabase(
    ConnectionConfig(
        host     = "localhost",
        port     = 5432,
        user     = "myuser",
        password = "secret",
        database = "mydb",
        sslMode  = SslMode.Prefer,
    )
)
val pool = database.pool()

// autocommit — connection acquired per call
pool {
    database.query("SELECT id, name FROM users WHERE id = :id")
        .bind("id" to 42)
        .single()
        .map { row -> User(row.get<Int>("id"), row.get<String>("name")) }
        .await().getOrThrow()
}

// transactional
pool.transactionally<Unit> {
    database.command("INSERT INTO orders (user_id, total) VALUES (:userId, :total)")
        .bind("userId" to 42, "total" to 99.99)
        .count()
        .await().getOrThrow()
    database.command("UPDATE users SET order_count = order_count + 1 WHERE id = :userId")
        .bind("userId" to 42)
        .count()
        .await().getOrThrow()
}

// streaming — rows streamed with backpressure
pool.transaction {
    database.query("SELECT * FROM events WHERE stream_id = :id ORDER BY sequence")
        .bind("id" to streamId)
        .multiple()
}
```

## Query API

```kotlin
db.query("...").bind("key" to value).single { row -> ... }      // One<T>   — exactly one row, error if 0 or 2+
db.query("...").bind("key" to value).multiple { row -> ... }    // Many<T>  — 0..N rows
db.query("...").bind("key" to value).optional { row -> ... }    // Maybe<T> — 0 or 1 row, error if 2+
db.command("...").bind("key" to value).count()                  // One<Long> — affected rows
db.effect("...").bind("key" to value).execute()                 // None<Unit>
```

Named parameters use `:name` syntax. They are rewritten to `$n` positional parameters at bind time. Binary wire format is used by default; per-codec negotiation selects text format where binary is unavailable.

## Type codec support

| Kotlin / Java type | PostgreSQL type |
|---|---|
| `Boolean` | `bool` |
| `Short` | `int2` |
| `Int` | `int4` |
| `Long` | `int8` |
| `Float` | `float4` |
| `Double` | `float8` |
| `BigDecimal` | `numeric` |
| `String` | `text`, `varchar` |
| `ByteArray` | `bytea` |
| `UUID` | `uuid` |
| `LocalDate` | `date` |
| `LocalTime` | `time` |
| `OffsetTime` | `timetz` |
| `LocalDateTime` | `timestamp` |
| `Instant` | `timestamptz` |
| `Duration` | `interval` |
| `Array<T>` / `List<T>` | arrays of any scalar type above |
| `@Serializable` | `json`, `jsonb` (via kotlinx.serialization) |

## Row access

```kotlin
row.boolean("col")
row.short("col")
row.int("col")
row.long("col")
row.float("col")
row.double("col")
row.decimal("col")
row.string("col")
row.bytes("col")
row.uuid("col")
row.date("col")
row.time("col")
row.offsetTime("col")
row.dateTime("col")
row.instant("col")
row.duration("col")
row.array<T>("col")
row.json<T>("col")       // kotlinx.serialization deserialization
```

Nullable variants are available for all accessors (`row.intOrNull("col")`, etc.).

## Status

| Phase | Description | Status |
|---|---|---|
| 1 | Codec layer, typed rows, named parameters, connection pool | Complete |
| 2 | Transaction API — `transactionally`, savepoints, isolation levels | Complete |

| 3 | Statement caching, named portals, `RETURNING` | Pending |
| 4 | `LISTEN` / `NOTIFY` | Pending |
| 5 | TLS (`SslMode`), session parameters (`searchPath`, `timezone`, `statementTimeout`, `lockTimeout`), SQLSTATE mapping, `CancelRequest`, `ParameterStatus`, `NoticeResponse` | Complete. SCRAM-SHA-256-PLUS pending. |

Tested against PostgreSQL 13, 15, 17, and 18beta2.

## ConnectionConfig fields

| Field | Type | Default | Purpose |
|---|---|---|---|
| `host` | `String` | — | Server hostname or IP |
| `port` | `Int` | `5432` | Server port |
| `user` | `String` | — | Authentication username |
| `password` | `String` | — | Authentication password |
| `database` | `String` | `user` | Database name |
| `sslMode` | `SslMode` | `SslMode.Prefer` | TLS policy |
| `applicationName` | `String` | `"minamoto"` | Appears in `pg_stat_activity` |
| `searchPath` | `List<String>` | `emptyList()` | Schema search order; empty means server default |
| `timezone` | `String?` | `null` | Session timezone; null means server default |
| `statementTimeout` | `Duration?` | `null` | Abort statements exceeding this duration |
| `lockTimeout` | `Duration?` | `null` | Abort lock waits exceeding this duration |
| `idleInTransactionSessionTimeout` | `Duration?` | `null` | Terminate sessions idle inside a transaction after this duration |
| `defaultFetchSize` | `Int` | `50` | Maximum rows per `Execute` round-trip |

## Error handling

All driver exceptions extend `MinamotoException`. SQLSTATE codes are mapped to typed subclasses:

| Exception type | SQLSTATE | Condition |
|---|---|---|
| `UniqueViolation` | `23505` | Unique constraint violation |
| `ForeignKeyViolation` | `23503` | Foreign key constraint violation |
| `NotNullViolation` | `23502` | Not-null constraint violation |
| `CheckViolation` | `23514` | Check constraint violation |
| `SerializationFailure` | `40001` | Serialization failure (retry-able) |
| `ServerDeadlockDetected` | `40P01` | Deadlock detected by the server |
| `QueryCancelled` | `57014` | Query cancelled via `CancelRequest` |
| `SyntaxError` | `42601` | SQL syntax error |
| `UndefinedTable` | `42P01` | Referenced table does not exist |
| `UndefinedColumn` | `42703` | Referenced column does not exist |

```kotlin
try {
    pool {
        database.command("INSERT INTO users (email) VALUES (:email)")
            .bind("email" to "duplicate@example.com")
            .count()
            .await().getOrThrow()
    }
} catch (e: MinamotoException.UniqueViolation) {
    // handle duplicate
} catch (e: MinamotoException) {
    // handle everything else
}
```
