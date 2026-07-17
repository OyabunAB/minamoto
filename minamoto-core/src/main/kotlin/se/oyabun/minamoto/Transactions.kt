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
@file:OptIn(ExperimentalTypeInference::class)
package se.oyabun.minamoto

import kotlin.coroutines.coroutineContext
import kotlin.experimental.ExperimentalTypeInference
import se.oyabun.aelv.Many
import se.oyabun.aelv.One
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.or
import se.oyabun.aelv.toMany

/**
 * Wraps [block] in a transaction on the pool currently installed in the coroutine context.
 *
 * Reads the active [ConnectionPool] from [PoolContext]. Nested calls on the same pool
 * produce savepoints rather than new transactions, allowing partial rollback via [recover]
 * on the inner pipeline without affecting the outer transaction.
 *
 * Services use this without any pool reference — the pool is resolved at subscription time
 * from whatever the caller installed in the context.
 */
/**
 * Wraps [block] in a transaction using [TransactionDefinition] resolved from the coroutine
 * context, falling back to [TransactionDefinition.ReadCommitted] if none is installed.
 *
 * Reads the active [ConnectionPool] from [PoolContext]. Nested calls on the same pool
 * produce savepoints rather than new transactions, allowing partial rollback via [recover]
 * on the inner pipeline without affecting the outer transaction.
 */
@OverloadResolutionByLambdaReturnType
fun <T : Any> transactionally(block: () -> One<T>): One<T> =
    Many.defer(factory = suspend {
        val pool = coroutineContext[PoolContext]?.pool
            ?: throw DatabaseException.InvalidState("no pool in context")
        pool.transactionally(transactionDefinition(), block).toMany()
    }).firstMaybe().or { throw DatabaseException.InvalidState("transactionally block returned no value") }

@OverloadResolutionByLambdaReturnType
fun <T : Any> transactionally(block: () -> Many<T>): Many<T> =
    Many.defer(factory = suspend {
        val pool = coroutineContext[PoolContext]?.pool
            ?: throw DatabaseException.InvalidState("no pool in context")
        pool.transactionally(transactionDefinition(), block)
    })

/** Explicit [definition] overrides any [TransactionDefinitionContext] in the coroutine context. */
@OverloadResolutionByLambdaReturnType
@JvmName("transactionallyWithDefinitionOne")
fun <T : Any> transactionally(definition: TransactionDefinition, block: () -> One<T>): One<T> =
    Many.defer(factory = suspend {
        val pool = coroutineContext[PoolContext]?.pool
            ?: throw DatabaseException.InvalidState("no pool in context")
        pool.transactionally(definition, block).toMany()
    }).firstMaybe().or { throw DatabaseException.InvalidState("transactionally block returned no value") }

@OverloadResolutionByLambdaReturnType
@JvmName("transactionallyWithDefinitionMany")
fun <T : Any> transactionally(definition: TransactionDefinition, block: () -> Many<T>): Many<T> =
    Many.defer(factory = suspend {
        val pool = coroutineContext[PoolContext]?.pool
            ?: throw DatabaseException.InvalidState("no pool in context")
        pool.transactionally(definition, block)
    })

/**
 * Returns the [TransactionDefinition] from the coroutine context if one is installed,
 * otherwise returns the default [TransactionDefinition].
 */
suspend fun transactionDefinition(): TransactionDefinition =
    coroutineContext[TransactionDefinitionContext]?.definition ?: TransactionDefinition()
