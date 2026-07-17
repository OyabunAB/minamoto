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

import kotlin.coroutines.CoroutineContext
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One

/**
 * Carries the active pool in the coroutine context.
 *
 * Installed by [se.oyabun.minamoto.pool.MinamotoPool.invoke] and
 * [se.oyabun.minamoto.pool.MinamotoPool.transactionally]. The innermost enclosing pool wins
 * when scopes are nested — each installation replaces the previous value in the context.
 *
 * Pipelines that need a connection read this element to acquire one from [pool].
 */
class PoolContext(val pool: ConnectionPool) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*> get() = PoolContext

    companion object : CoroutineContext.Key<PoolContext>
}

/**
 * Minimal pool interface visible to [minamoto-core].
 *
 * The full implementation lives in [se.oyabun.minamoto.pool.MinamotoPool].
 * This interface exists so [PoolContext] can carry the pool without a circular dependency.
 */
interface ConnectionPool {
    val poolId: PoolId
    fun acquire(): One<ConnectionAcquireResult>
    fun release(id: ConnectionId): None<Unit>
    fun <T : Any> transactionally(definition: TransactionDefinition, block: () -> One<T>): One<T>
    fun <T : Any> transactionally(definition: TransactionDefinition, block: () -> Many<T>): Many<T>
}

/**
 * Result of a [ConnectionPool.acquire] call.
 */
sealed interface ConnectionAcquireResult {
    data class  Acquired(val connection: Connection)                     : ConnectionAcquireResult
    data object TimedOut                                                  : ConnectionAcquireResult
    data class  DeadlockDetected(val held: Int, val poolSize: Int)       : ConnectionAcquireResult
}
