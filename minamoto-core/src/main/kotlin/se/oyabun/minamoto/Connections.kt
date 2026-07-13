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

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

@JvmInline
value class ConnectionId(val value: Long)

@JvmInline
value class TransactionId(val value: Long)

@JvmInline
value class SavepointId(val value: String)

// ---------------------------------------------------------------------------
// Connection state
// ---------------------------------------------------------------------------

sealed interface ConnectionState {
    data object Idle        : ConnectionState
    data object Acquired    : ConnectionState
    data object Executing   : ConnectionState
    data object InTransaction : ConnectionState
    data object Closing     : ConnectionState
    data object Closed      : ConnectionState
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

sealed interface TransactionMode {
    /** Use existing transaction from context if present, otherwise start a new one. */
    data object Join : TransactionMode
    /** Always start a new transaction. Nested becomes a savepoint. */
    data object New  : TransactionMode
}

sealed interface IsolationLevel {
    data object ReadUncommitted  : IsolationLevel
    data object ReadCommitted    : IsolationLevel
    data object RepeatableRead   : IsolationLevel
    data object Serializable     : IsolationLevel
}

sealed interface TransactionMutability {
    data object ReadWrite : TransactionMutability
    data object ReadOnly  : TransactionMutability
}

data class TransactionDefinition(
    val isolation:   IsolationLevel        = IsolationLevel.ReadCommitted,
    val mutability:  TransactionMutability = TransactionMutability.ReadWrite,
    val deferrable:  Boolean               = false,
)

sealed interface TransactionState {
    data object Active     : TransactionState
    data object Committed  : TransactionState
    data object RolledBack : TransactionState
}

sealed interface TransactionBoundary {
    /** Top-level transaction — commit or rollback on exit. */
    data class Root(
        val id:         TransactionId,
        val definition: TransactionDefinition,
        val state:      TransactionState = TransactionState.Active,
    ) : TransactionBoundary

    /** Nested transaction — maps to a savepoint on the enclosing connection. */
    data class Savepoint(
        val id:         SavepointId,
        val state:      TransactionState = TransactionState.Active,
    ) : TransactionBoundary
}

// ---------------------------------------------------------------------------
// Connection context — lives on the CoroutineContext
// ---------------------------------------------------------------------------

/**
 * Tracks all connections held by the current coroutine chain.
 *
 * Shared by reference across flatMap fan-outs so the pool can detect
 * potential deadlocks before they occur.
 *
 * [stack] is the current nesting level — each transaction push adds a frame.
 * [held] is shared across all frames and all concurrent children.
 */
class ConnectionContext(
    val stack: ConnectionFrame?,
    val held:  ConcurrentHashMap<ConnectionId, Int> = ConcurrentHashMap(),
) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*> get() = ConnectionContext

    companion object : CoroutineContext.Key<ConnectionContext>

    fun push(frame: ConnectionFrame): ConnectionContext =
        ConnectionContext(frame, held)

    fun pop(): ConnectionContext =
        ConnectionContext(stack?.parent, held)

    fun acquire(id: ConnectionId) {
        held.merge(id, 1, Int::plus)
    }

    fun release(id: ConnectionId) {
        held.compute(id) { _, count -> if (count == null || count <= 1) null else count - 1 }
    }

    fun holds(id: ConnectionId): Boolean = held.containsKey(id)
}

/**
 * A single frame on the connection stack.
 *
 * [connection] is the raw connection at this level.
 * [transaction] is the active transaction boundary at this level, if any.
 * [parent] is the enclosing frame.
 */
data class ConnectionFrame(
    val connection:  ConnectionId,
    val transaction: TransactionBoundary?,
    val parent:      ConnectionFrame?,
)
