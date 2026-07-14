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
val db: Database = PostgresDatabase(
    PostgresConnectionFactory(host = "localhost", port = 5432, database = "mydb", user = "myuser", password = "secret"),
    MinamotoPool(minSize = 2, maxSize = 10)
)

// Single row
val user: User = db.query("SELECT id, name FROM users WHERE id = :id")
    .bind("id" to 42)
    .single { row -> User(row.int("id"), row.string("name")) }
    .get()

// Multiple rows
val users: List<User> = db.query("SELECT id, name FROM users WHERE active = :active")
    .bind("active" to true)
    .multiple { row -> User(row.int("id"), row.string("name")) }
    .toList()
    .get()

// Optional row
val maybeUser: Maybe<User> = db.query("SELECT id, name FROM users WHERE email = :email")
    .bind("email" to "user@example.com")
    .optional { row -> User(row.int("id"), row.string("name")) }

// DML — affected row count
val count: Long = db.command("UPDATE users SET active = :active WHERE id = :id")
    .bind("active" to false, "id" to 42)
    .count()
    .get()

// Side-effect — no return value
db.effect("DELETE FROM sessions WHERE expires_at < :now")
    .bind("now" to Instant.now())
    .execute()
    .await()
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
| 2 | Transaction API | Pending |
| 3 | Statement caching, named portals, `RETURNING` | Pending |
| 4 | `LISTEN` / `NOTIFY` | Pending |
| 5 | TLS, session parameters | Pending |

Tested against PostgreSQL 13, 15, 17, and 18beta2.
