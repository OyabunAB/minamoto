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
package se.oyabun.minamoto

import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import se.oyabun.aelv.None
import se.oyabun.aelv.One

@JvmInline
value class ConnectionId(val value: Long)

@JvmInline
value class TransactionId(val value: Long)

@JvmInline
value class SavepointId(val value: String)

/** Stable identity for a pool instance — used to scope deadlock detection to a specific pool. */
@JvmInline
value class PoolId(val value: Long)

sealed interface ConnectionState {
    data object Idle          : ConnectionState
    data object Acquired      : ConnectionState
    data object Executing     : ConnectionState
    data object InTransaction : ConnectionState
    data object Closing       : ConnectionState
    data object Closed        : ConnectionState
}

/**
 * Whether a connection passed a health check.
 *
 * [Invalid.reason] contains a human-readable description suitable for logging.
 * The pool discards an [Invalid] connection and creates a replacement.
 */
sealed interface ValidationResult {
    data object Valid                       : ValidationResult
    data class  Invalid(val reason: String) : ValidationResult
}

/**
 * Controls how [Database.transaction] behaves when a transaction is already active
 * on the coroutine context.
 *
 * [Join] is the default and covers most use cases — services can call each other
 * freely and all participate in the same transaction without coordination.
 *
 * [New] is for operations that must succeed or fail independently, such as audit
 * logging or outbox writes that must not roll back with the outer transaction.
 * When a transaction is already active, [New] creates a savepoint rather than
 * a full nested transaction.
 */
sealed interface TransactionMode {
    data object Join : TransactionMode
    data object New  : TransactionMode
}

sealed interface IsolationLevel {
    data object ReadUncommitted : IsolationLevel
    data object ReadCommitted   : IsolationLevel
    data object RepeatableRead  : IsolationLevel
    data object Serializable    : IsolationLevel
}

sealed interface TransactionMutability {
    data object ReadWrite : TransactionMutability
    data object ReadOnly  : TransactionMutability
}

/**
 * Options applied when starting a transaction.
 *
 * [deferrable] is PostgreSQL-specific — only meaningful with [IsolationLevel.Serializable]
 * and [TransactionMutability.ReadOnly]. It allows the server to delay the transaction
 * until it can run without blocking or being blocked.
 */
data class TransactionDefinition(
    val isolation:  IsolationLevel        = IsolationLevel.ReadCommitted,
    val mutability: TransactionMutability = TransactionMutability.ReadWrite,
    val deferrable: Boolean               = false,
)

/**
 * Carries the active [TransactionDefinition] in the coroutine context.
 *
 * Installed by callers that want all nested [transactionally] calls to use a specific
 * isolation level, mutability, or deferrable setting without passing it explicitly.
 * The innermost [TransactionDefinitionContext] wins.
 */
class TransactionDefinitionContext(val definition: TransactionDefinition) : kotlin.coroutines.CoroutineContext.Element {
    override val key: kotlin.coroutines.CoroutineContext.Key<*> get() = TransactionDefinitionContext
    companion object : kotlin.coroutines.CoroutineContext.Key<TransactionDefinitionContext>
}

sealed interface TransactionState {
    data object Active     : TransactionState
    data object Committed  : TransactionState
    data object RolledBack : TransactionState
}

/**
 * The active transaction boundary at a given [ConnectionStack.Frame].
 *
 * [None] means the connection is held outside of any transaction.
 * [Root] is a top-level transaction — committed or rolled back on block exit.
 * [Savepoint] is a nested transaction within an enclosing [Root].
 */
sealed interface TransactionBoundary {
    data object None : TransactionBoundary

    data class Root(
        val id:         TransactionId,
        val definition: TransactionDefinition,
        val state:      TransactionState = TransactionState.Active,
    ) : TransactionBoundary

    data class Savepoint(
        val id:    SavepointId,
        val state: TransactionState = TransactionState.Active,
    ) : TransactionBoundary
}

/**
 * The connection stack carried by [ConnectionContext].
 *
 * [Empty] is the base case — no connections held, no transaction active.
 * Each [Database.transaction] call or connection acquisition pushes a [Frame].
 * On exit the frame is popped, and the connection is released or the savepoint
 * resolved depending on [Frame.transaction].
 */
sealed interface ConnectionStack {
    data object Empty : ConnectionStack

    data class Frame(
        val connection:  ConnectionId,
        val poolId:      PoolId,
        val transaction: TransactionBoundary,
        val parent:      ConnectionStack,
    ) : ConnectionStack
}

/**
 * A single physical connection to the database server.
 *
 * Implemented by each driver. The pool holds instances of it.
 * Callers never interact with this directly — they go through [Database].
 */
interface Connection {
    val id:    ConnectionId
    val state: ConnectionState
    fun ping(): One<ValidationResult>
    fun close(): None<Unit>

    /** Send `BEGIN` with [definition]'s isolation level, mutability, and deferrable flag. */
    fun begin(definition: TransactionDefinition = TransactionDefinition()): None<Unit>

    /** Send `COMMIT`. */
    fun commit(): None<Unit>

    /** Send `ROLLBACK`. */
    fun rollback(): None<Unit>

    /** Send `SAVEPOINT [id]`. */
    fun savepoint(id: SavepointId): None<Unit>

    /** Send `RELEASE SAVEPOINT [id]`. */
    fun releaseSavepoint(id: SavepointId): None<Unit>

    /** Send `ROLLBACK TO SAVEPOINT [id]`. */
    fun rollbackToSavepoint(id: SavepointId): None<Unit>
}

/**
 * Creates, validates, and destroys physical [Connection]s.
 *
 * Each driver implements this interface and passes it to the pool at construction time.
 * The pool calls [create] when it needs a new slot, [validate] before handing out a
 * connection (subject to the configured [se.oyabun.minamoto.pool.ValidationQuery]), and
 * [destroy] when evicting or shutting down.
 */
interface ConnectionFactory {
    fun create(): One<Connection>
    fun validate(connection: Connection): One<ValidationResult>
    fun destroy(connection: Connection): None<Unit>
}

/**
 * Tracks all connections held by the current coroutine chain.
 *
 * A single [ConnectionContext] instance is shared across all coroutines spawned from
 * the same chain — including [se.oyabun.aelv.Many.flatMap] fan-outs — so the pool
 * always has a complete view of what this chain holds. This is what makes deadlock
 * detection reliable under recursive flatMap pipelines.
 *
 * [stack] reflects the current transaction nesting level.
 * [held] maps each acquired [ConnectionId] to the [PoolId] it was borrowed from.
 * The pool uses this to detect when the current chain already holds all slots of its
 * own pool — preventing a guaranteed self-deadlock before blocking on acquisition.
 */
class ConnectionContext(
    val stack: ConnectionStack = ConnectionStack.Empty,
    val held:  ConcurrentHashMap<ConnectionId, PoolId> = ConcurrentHashMap(),
) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*> get() = ConnectionContext

    companion object : CoroutineContext.Key<ConnectionContext>

    fun push(frame: ConnectionStack.Frame): ConnectionContext = ConnectionContext(frame, held)

    fun pop(): ConnectionContext = ConnectionContext(
        when (val s = stack) {
            is ConnectionStack.Empty -> ConnectionStack.Empty
            is ConnectionStack.Frame -> s.parent
        },
        held,
    )

    fun acquire(id: ConnectionId, poolId: PoolId) { held[id] = poolId }
    fun release(id: ConnectionId)                 { held.remove(id) }
    fun holds(id: ConnectionId): Boolean          = held.containsKey(id)

    /** Number of connections currently held from [poolId] by this coroutine chain. */
    fun heldCountFor(poolId: PoolId): Int = held.values.count { it == poolId }

    /** The [ConnectionId] of the innermost active transaction on [stack], or null if none. */
    fun activeConnectionId(): ConnectionId? = when (val s = stack) {
        is ConnectionStack.Empty -> null
        is ConnectionStack.Frame -> s.connection
    }

    /** The [PoolId] of the innermost active transaction on [stack], or null if none. */
    fun activePoolId(): PoolId? = when (val s = stack) {
        is ConnectionStack.Empty -> null
        is ConnectionStack.Frame -> s.poolId
    }
}

