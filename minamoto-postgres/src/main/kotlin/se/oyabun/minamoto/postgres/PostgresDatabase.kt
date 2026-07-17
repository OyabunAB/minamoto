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

import kotlinx.coroutines.sync.Semaphore
import se.oyabun.minamoto.Database
import se.oyabun.minamoto.TransactionDefinition
import se.oyabun.minamoto.IsolationLevel
import se.oyabun.minamoto.TransactionMutability
import se.oyabun.minamoto.postgres.codec.CodecRegistry
import se.oyabun.minamoto.pool.MinamotoPool
import se.oyabun.minamoto.pool.PoolConfig

/**
 * Entry point for a PostgreSQL database.
 *
 * Holds connection configuration and a shared [connectionBudget] that enforces the total
 * live connection ceiling across all pools created from this instance. The budget prevents
 * collective pool over-subscription beyond [maxConnections].
 *
 * Call [pool] to create a [MinamotoPool] backed by this database. Multiple pools may be
 * created (e.g. one for writes, one for reads) — they all draw from the same budget.
 */
class PostgresDatabase(
    private val config:         ConnectionConfig,
    private val maxConnections: Int          = 90,
    private val registry:       CodecRegistry = CodecRegistry(),
) : Database {

    private val connectionBudget = Semaphore(maxConnections)

    /**
     * Creates a [MinamotoPool] backed by this database.
     *
     * All pools share [connectionBudget] — the total live connections across all pools
     * will never exceed [maxConnections].
     */
    fun pool(poolConfig: PoolConfig = PoolConfig()): MinamotoPool =
        MinamotoPool(
            config           = poolConfig,
            factory          = PostgresConnectionFactory(config, registry),
            connectionBudget = connectionBudget,
        )

    override fun query(statement: String)   = PostgresQuery(registry, statement)
    override fun modify(statement: String) = PostgresModify(registry, statement)
    override fun run(statement: String)  = PostgresRun(registry, statement)
}

/** Builds the `BEGIN` SQL statement from a [TransactionDefinition]. */
internal fun TransactionDefinition.toBeginSql(): String = buildString {
    append("BEGIN")
    append(" ISOLATION LEVEL ")
    append(when (isolation) {
        IsolationLevel.ReadUncommitted -> "READ UNCOMMITTED"
        IsolationLevel.ReadCommitted   -> "READ COMMITTED"
        IsolationLevel.RepeatableRead  -> "REPEATABLE READ"
        IsolationLevel.Serializable    -> "SERIALIZABLE"
    })
    append(when (mutability) {
        TransactionMutability.ReadWrite -> " READ WRITE"
        TransactionMutability.ReadOnly  -> " READ ONLY"
    })
    if (deferrable) append(" DEFERRABLE")
}
