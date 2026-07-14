/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.minamoto.postgres

import se.oyabun.minamoto.Binding
import se.oyabun.minamoto.Database
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.TransactionMode

/**
 * [Database] implementation for PostgreSQL backed by a single [PostgresConnection].
 *
 * Named parameters use `:name` syntax — bind them with Kotlin's `to` infix: `"id" to 42`.
 * The SQL rewriter replaces `:name` with Postgres positional parameters (`$1`, `$2`, ...)
 * at bind time. Cast syntax (`::text`) is preserved.
 *
 * Transaction support is not yet implemented — [transaction] throws [UnsupportedOperationException].
 */
class PostgresDatabase internal constructor(internal val connection: PostgresConnection) : Database {

    override fun query(statement: String)   = connection.query(statement)
    override fun command(statement: String) = connection.command(statement)
    override fun effect(statement: String)  = connection.effect(statement)

    override suspend fun <T> transaction(
        mode:       TransactionMode,
        definition: TransactionDefinition,
        block:      suspend () -> T,
    ): T = throw UnsupportedOperationException("Transaction support not yet implemented — Phase 2")
}
