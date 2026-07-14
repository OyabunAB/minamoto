# Minamoto — Development Plan

## Vision

Reactor-free PostgreSQL driver. `aelv` is the reactive runtime. No R2DBC SPI compliance
is a goal — minamoto defines its own clean API. The driver must be production-grade:
correct, fast, and free of leaky abstractions.

---

## Status

### Done
- `aelv` — `UnicastSink`, `scan`, `discard`, `Verify`, suspend overloads, all operators
- `aelv-netty` — `NettyTransport`, `InboundHandler`, `NettyDispatchers`
- `minamoto-core` — `Database`, `Query`, `Command`, `Effect`, `Batch`, `Row`, `RowMetadata`,
  `ColumnMetadata`, `ColumnType`, `Nullability`, `MinamotoException`, connection SPI
- `minamoto-pool` — `MinamotoPool`, `PoolConfig`, eviction, hooks, deadlock detection,
  full test suite (17 tests)
- `minamoto-postgres` — PGwire encoder/decoder/framer, SCRAM-SHA-256/MD5/trust,
  extended query protocol, `PgConnection`, `PgConnectionFactory`, 30 tests passing

---

## Roadmap

Items are ordered by dependency. Each phase must be complete and tested before the next begins.

---

### Phase 1 — Codec layer  *(next)*

**Why it's first:** `Row.get<T>()` and `Command`/`Query` parameter binding both need codecs.
Nothing above this layer is usable without it.

**Scope (TBD via interview):**
- `Codec<T>` interface — encode (Kotlin value → `ByteArray`, format code) + decode (`ByteArray?`, OID → T)
- `CodecRegistry` — lookup by OID + Kotlin type; pluggable
- Built-in codecs: boolean, int2/4/8, float4/8, numeric/BigDecimal, text/varchar,
  bytea, uuid, date, time, timetz, timestamp, timestamptz, interval, json, jsonb, hstore (map)
- Array codecs for all scalar types above
- Binary format negotiation in `Bind` (format codes per parameter, per result column)
- `Row` implementation backed by `ColumnDescription` + codec dispatch
- Parameter encoding: `Any` → `ByteArray?` + format code via registry
- Custom codec registrar for user-defined types (enums, pgvector, domain types)

**Open questions (interview):**
- Binary vs text format per codec — default to binary for everything, or text for safety?
- How should the codec registry resolve ambiguity (same OID, multiple Kotlin types)?
- Should `Row.get<T>()` accept a reified type parameter or a `KClass<T>` argument?
- Null semantics — sentinel, exception, or `getOrNull` always required for nullable?
- Where does the codec live — `minamoto-core`, `minamoto-postgres`, or a new module?
- Do we want a `json`/`jsonb` abstraction or raw `ByteArray` + caller-side parsing?

---

### Phase 2 — Transaction API

- `PgConnection.beginTransaction()` / `commitTransaction()` / `rollbackTransaction()`
- `setAutoCommit(boolean)`
- `TransactionDefinition` (isolation level, read-only, deferrable) sent in `BEGIN`
- Savepoints: `createSavepoint(name)` / `releaseSavepoint(name)` / `rollbackToSavepoint(name)`
- `Database.transaction {}` implementation wired to real `BEGIN`/`COMMIT`/`ROLLBACK`
- `TransactionStatus` from `ReadyForQuery` exposed on `PgConnection`

---

### Phase 3 — Statement & Result SPI

- `Statement` builder: `bind(index, value)`, `bind(name, value)`, `bindNull`, `add()` batch, `fetchSize`
- `Result` — typed `rows(): Many<Row>`, `rowsUpdated(): One<Long>`
- `RowMetadata` wired from live `RowDescription`
- `RETURNING` support in `Command`
- Named prepared statement cache (server-side, keyed by SQL hash)
- Named portals (required for concurrent statements per connection)
- `Flush` message between pipeline stages

---

### Phase 4 — LISTEN/NOTIFY

- Surface `NotificationResponse` as `Many<Notification>` on `PgConnection`
- `LISTEN`/`UNLISTEN` helpers
- Idle-connection notification poll (no-op `Sync` to drain notifications on a parked connection)

---

### Phase 5 — Protocol completeness

- TLS: `SSLMode` (DISABLE / ALLOW / PREFER / REQUIRE / VERIFY_CA / VERIFY_FULL)
- SCRAM-SHA-256-PLUS (channel binding)
- `CancelRequest` — actually send it on coroutine cancellation
- Session params in `StartupMessage` (search_path, timezone, lock_wait_timeout, options map)
- `applicationName` configurable
- `ParameterStatus` updates surfaced to callers
- `NoticeResponse` surfaced with configurable log level
- SQLSTATE → `MinamotoException` subtype mapping

---

### Phase 6 — Connection management

- Credential supplier (`() -> String` for rotation)
- Multi-host / fail-over (`PRIMARY` / `SECONDARY` / `ANY` strategy)
- TCP tuning (`tcpNoDelay`, `tcpKeepAlive`)
- Unix domain socket transport

---

### Phase 7 — Logical replication  *(optional / deferred)*

- Replication connection mode
- Logical / physical replication slots
- WAL streaming + keepalive/feedback protocol

---

## Constraints (permanent)

- No abbreviations in identifiers (`connection` not `conn`, `buffer` not `buf`)
- No nulls in public API — sealed types and sentinels
- No `flow {}` — use aelv primitives (`Many`, `One`, `None`, `Sinks`)
- No `forEach` operator
- All errors through `MinamotoException` sealed hierarchy
- `runBlocking` (not `runTest`) for tests involving real network I/O
- KDoc only where it explains *why* or *what*, never restates the name
- Commits signed with `-S`
