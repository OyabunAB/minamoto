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

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Coroutine dispatchers for minamoto-postgres.
 *
 * All minamoto work runs on named threads, making activity visible in thread dumps,
 * profilers, and distributed traces. Crossings into aelv-netty (connect, write, inbound)
 * happen on NettyDispatchers and return here on completion.
 */
internal object MinamotoDispatchers {

    /**
     * General-purpose dispatcher for connection lifecycle, protocol processing,
     * and query execution.
     */
    val connection: CoroutineDispatcher = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors(),
    ) { runnable ->
        Thread(runnable, "minamoto-connection-${counter.incrementAndGet()}")
            .also { it.isDaemon = true }
    }.asCoroutineDispatcher()

    private val counter = AtomicInteger(0)
}
