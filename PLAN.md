# Minamoto — Development Plan

## Vision

Reactor-free PostgreSQL driver. `aelv` is the reactive runtime. The driver must be
production-grade: correct, fast, and free of leaky abstractions. R2DBC SPI compliance
is a goal — minamoto should be a drop-in R2DBC driver usable with Spring Data R2DBC,
jOOQ reactive, and any other R2DBC-aware framework, while also exposing its own clean
higher-level API directly.

---

## Status

### Done

- `aelv` — `UnicastSink`, `scan`, `discard`, `Verify`, suspend overloads, all operators
- `aelv-netty` — `NettyTransport`, `InboundHandler`, `NettyDispatchers`
- `minamoto-core` — `Database`, `Query`, `Command`, `Effect`, `Batch`, `Row`, `RowMetadata`,
  `ColumnMetadata`, `ColumnType`, `Nullability`, `MinamotoException`, connection SPI,
  `PauseBehavior`, `NotificationChannel`, `NotificationSerializer`, `Listener`, `Notifier`
- `minamoto-pool` — `MinamotoPool`, `PoolConfig`, eviction, hooks, deadlock detection,
  full test suite
- `minamoto-postgres` — PGwire encoder/decoder/framer, SCRAM-SHA-256/MD5/SCRAM-PLUS/trust,
  extended query protocol, named portals, named prepared statement cache (LRU),
  true backpressure streaming (Execute+Flush+Close), `PostgresDatabase`, `PostgresConnection`
- **Phase 1 — Codec layer** — `Codec<T>`, `CodecRegistry` (OID+type lookup, numeric widening,
  type-only lazy binding, `Any` dispatch, `String` fallback, supertype encode fallback),
  `CodecRegistrar` SPI; built-in scalar codecs (bool, int2/4/8, float4/8, numeric, text,
  bytea, uuid, date, time, timetz, timestamp, timestamptz, interval); array codecs for all
  scalar types; N-dimensional arrays via flat read + recursive reshape; JSON/JSONB via
  kotlinx.serialization; `registerJson`, `registerJsonb`, `registerEnum`, `registerVector`,
  `registerHstore`, `registerByType`
- **Phase 1 — Extended types** — `InetAddressCodec` (inet/inet[]), geometric types
  (`PgPoint`, `PgBox`, `PgCircle`, `PgLine`, `PgLseg`, `PgPath`, `PgPolygon`) with array
  codecs, `HstoreCodec` (`Map<String,String?>`, binary + text fallback),
  `PgVectorCodec` (`FloatArray`, binary + text fallback), `DynamicArrayCodec` for
  user-defined type arrays (enum[], lazy element OID resolution)
- **Phase 2 — Transaction API** — `BEGIN`/`COMMIT`/`ROLLBACK`, `TransactionDefinition`
  (isolation, read-only, deferrable), savepoints, `Database.transaction {}`,
  `TransactionStatus` from `ReadyForQuery`
- **Phase 3 — Statement & Result SPI** — named parameters (`:name` rewriting), `fetchSize`,
  `RETURNING`, named prepared statement cache (LRU, per-connection), named portals,
  `Flush` between pipeline stages, concurrent statement tests
- **Phase 4 — LISTEN/NOTIFY** — `PostgresListener` (dedicated connection, exponential
  back-off reconnect, JVM shutdown hook), `PostgresNotifier`, `PauseBehavior.Buffer`
  (client-side queue, configurable `maxSize`/`overflow`) and `PauseBehavior.Discard`,
  `database.listener()` / `database.notifier()`
- **Phase 5 — Protocol completeness** — TLS (`SslMode` DISABLE→VERIFY_FULL),
  SCRAM-SHA-256-PLUS (channel binding), `CancelRequest`, typed `ConnectionConfig` session
  params, `ParameterStatus` updates, `NoticeResponse`, SQLSTATE → `MinamotoException` subtypes
- **Phase 6 — Connection management** — credential rotation supplier, multi-host PRIMARY
  strategy, `acquiredConnections()` on `ManagedPool`

---

## Roadmap

---

### COPY FROM STDIN  *(next)*

Bulk insert via the PGwire `COPY` protocol, binary format.

- New wire messages: `CopyInResponse`, `CopyData`, `CopyDone`, `CopyFail`
- Binary COPY format: file header, per-row header, column values as binary codec output,
  file trailer
- `database.copyIn(table, columns, registry)` → returns a sink/stream that accepts rows
  and flushes `CopyData` frames
- `CommandComplete` tag parsed for inserted row count
- Error path: `CopyFail` sent on pipeline cancellation or upstream error

---

### R2DBC SPI compliance

Adapter layer that implements `io.r2dbc.spi.*` on top of minamoto's existing internals.
`aelv` publishers bridge to `org.reactivestreams.Publisher` via the existing `asFlow()`
path; the protocol and pool machinery are unchanged.

- `MinamotoConnectionFactory` implements `ConnectionFactory`
- `MinamotoConnection` implements `Connection` — wraps `PostgresConnection`
- `MinamotoStatement` implements `Statement` — delegates to `PostgresQuery`/`PostgresModify`
- `MinamotoResult` implements `Result` — adapts `Many<Row>` and `rowsUpdated`
- `MinamotoRow` / `MinamotoRowMetadata` implement `Row` / `RowMetadata`
- `META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider` for auto-discovery
- R2DBC TCK passing

---

### Phase 7 — Logical replication  *(optional / deferred)*

- Replication connection mode
- Logical / physical replication slots
- WAL streaming + keepalive/feedback protocol

---

### Remaining extended type coverage

- `COPY FROM STDIN with binary stream inserts rows` — see above
- Geometric encode round-trips (all 7 types, currently decode-only tests)
- `hstore` null-value encode parameter test

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
