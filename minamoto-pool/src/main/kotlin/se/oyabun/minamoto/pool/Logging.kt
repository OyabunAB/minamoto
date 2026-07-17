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
package se.oyabun.minamoto.pool

import org.slf4j.LoggerFactory
import org.slf4j.Logger as Slf4jLogger
import se.oyabun.minamoto.ConnectionId
import se.oyabun.minamoto.SavepointId
import se.oyabun.minamoto.TransactionDefinition
import kotlin.time.Duration

internal inline fun Slf4jLogger.trace(message: () -> String) { if (isTraceEnabled) trace(message()) }
internal inline fun Slf4jLogger.debug(message: () -> String) { if (isDebugEnabled) debug(message()) }
internal inline fun Slf4jLogger.info(message: () -> String)  { if (isInfoEnabled)  info(message())  }
internal inline fun Slf4jLogger.warn(message: () -> String)  { if (isWarnEnabled)  warn(message())  }
internal inline fun Slf4jLogger.warn(cause: Throwable, message: () -> String)  { if (isWarnEnabled)  warn(message(), cause)  }
internal inline fun Slf4jLogger.error(cause: Throwable, message: () -> String) { if (isErrorEnabled) error(message(), cause) }
internal inline fun Slf4jLogger.debug(cause: Throwable, message: () -> String) { if (isDebugEnabled) debug(message(), cause) }

internal class Log(private val slf4j: Slf4jLogger) {

    val pool = Pool()

    inner class Pool {
        fun slotAcquired(id: ConnectionId)                             = slf4j.debug { "slot acquired [$id]" }
        fun slotReleased(id: ConnectionId)                             = slf4j.debug { "slot released [$id]" }
        fun slotInvalidated(id: ConnectionId, reason: String)          = slf4j.warn  { "slot invalidated [$id]: $reason" }
        fun slotCreated(id: ConnectionId)                              = slf4j.debug { "slot created [$id]" }
        fun deadlockPrevented(held: Int, maxSize: Int)                 = slf4j.warn  { "deadlock prevented: held=$held max=$maxSize" }
        fun acquireTimeout(timeout: Duration)                          = slf4j.warn  { "acquire timed out after $timeout" }
        fun transactionOpened(id: ConnectionId, definition: TransactionDefinition) =
            slf4j.debug { "transaction opened [$id]: $definition" }
        fun transactionClosed(id: ConnectionId, committed: Boolean)    =
            slf4j.debug { "transaction closed [$id]: ${if (committed) "committed" else "rolled back"}" }
        fun savepointCreated(id: ConnectionId, savepoint: SavepointId) =
            slf4j.debug { "savepoint created [$id]: ${savepoint.value}" }
        fun savepointRolledBack(id: ConnectionId, savepoint: SavepointId) =
            slf4j.debug { "savepoint rolled back [$id]: ${savepoint.value}" }
        fun savepointReleased(id: ConnectionId, savepoint: SavepointId) =
            slf4j.debug { "savepoint released [$id]: ${savepoint.value}" }
    }
}

internal object Logging {
    inline fun <reified T : Any> of(): Log = Log(LoggerFactory.getLogger(T::class.java))
}
