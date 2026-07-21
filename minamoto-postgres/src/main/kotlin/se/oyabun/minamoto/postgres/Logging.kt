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

import org.slf4j.LoggerFactory
import org.slf4j.Logger as Slf4jLogger
import se.oyabun.minamoto.ConnectionId

internal inline fun Slf4jLogger.trace(message: () -> String) { if (isTraceEnabled) trace(message()) }
internal inline fun Slf4jLogger.debug(message: () -> String) { if (isDebugEnabled) debug(message()) }
internal inline fun Slf4jLogger.info(message: () -> String)  { if (isInfoEnabled)  info(message())  }
internal inline fun Slf4jLogger.warn(message: () -> String)  { if (isWarnEnabled)  warn(message())  }
internal inline fun Slf4jLogger.warn(cause: Throwable, message: () -> String)  { if (isWarnEnabled)  warn(message(), cause)  }
internal inline fun Slf4jLogger.error(cause: Throwable, message: () -> String) { if (isErrorEnabled) error(message(), cause) }
internal inline fun Slf4jLogger.debug(cause: Throwable, message: () -> String) { if (isDebugEnabled) debug(message(), cause) }

internal class Log(private val slf4j: Slf4jLogger) {

    val connection = Connection()
    val protocol   = Protocol()
    val pool       = Pool()
    val query      = Query()

    inner class Connection {
        fun created(id: ConnectionId)                                = slf4j.debug { "connection created [$id]" }
        fun closed(id: ConnectionId)                                 = slf4j.debug { "connection closed [$id]" }
        fun <T> closed(id: ConnectionId, action: () -> T): T        { slf4j.debug { "connection closed [$id]" };   return action() }
        fun <T> closing(id: ConnectionId, action: () -> T): T       { slf4j.debug { "connection closing [$id]" };  return action() }
        fun error(id: ConnectionId, cause: Throwable)                = slf4j.error(cause) { "connection error [$id]" }
        fun invalidState(id: ConnectionId, text: String)             = slf4j.warn { "invalid state [$id]: $text" }
        fun notice(id: ConnectionId, severity: String, message: String) =
            slf4j.warn { "server notice [$id] $severity: $message" }
    }

    inner class Protocol {
        fun handshakeStarted(id: ConnectionId)                       = slf4j.debug { "handshake started [$id]" }
        fun handshakeComplete(id: ConnectionId)                      = slf4j.debug { "handshake complete [$id]" }
        fun <T> authRequired(id: ConnectionId, type: String, action: () -> T): T { slf4j.debug { "auth required [$id]: $type" }; return action() }
        fun messageReceived(id: ConnectionId, detail: String)        = slf4j.trace { "← [$id] $detail" }
        fun messageSent(id: ConnectionId, detail: String)            = slf4j.trace { "→ [$id] $detail" }
        fun parameterStatus(id: ConnectionId, name: String, value: String) =
            slf4j.trace { "← [$id] ParameterStatus $name=$value" }
        fun queryStarted(id: ConnectionId, sql: String)              = slf4j.debug { "query [$id]: ${sql.take(120)}" }
        fun queryComplete(id: ConnectionId)                          = slf4j.debug { "query complete [$id]" }
        fun conversationQueued(id: ConnectionId, size: Int)          = slf4j.trace { "conversation queued [$id] queue=$size" }
        fun conversationComplete(id: ConnectionId, remaining: Int)   = slf4j.trace { "conversation complete [$id] queue=$remaining" }
        fun noConversation(id: ConnectionId, text: String)           = slf4j.warn { "no active conversation [$id]: $text" }
    }

    inner class Pool {
        fun acquired(id: ConnectionId)                    = slf4j.debug { "acquired [$id]" }
        fun released(id: ConnectionId)                    = slf4j.debug { "released [$id]" }
        fun invalidated(id: ConnectionId, reason: String) = slf4j.warn { "invalidated [$id]: $reason" }
    }

    inner class Query {
        fun acquiredFromPool(id: ConnectionId)            = slf4j.trace { "acquired from pool [$id]" }
        fun reusingTransactionConnection(id: ConnectionId) = slf4j.trace { "reusing transaction connection [$id]" }
    }
}

internal object Logging {
    inline fun <reified T : Any> of(): Log = Log(LoggerFactory.getLogger(T::class.java))
}
