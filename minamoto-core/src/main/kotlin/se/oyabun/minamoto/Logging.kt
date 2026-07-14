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

import org.slf4j.LoggerFactory
import org.slf4j.Logger as Slf4jLogger

internal inline fun Slf4jLogger.trace(msg: () -> String) { if (isTraceEnabled) trace(msg()) }
internal inline fun Slf4jLogger.debug(msg: () -> String) { if (isDebugEnabled) debug(msg()) }
internal inline fun Slf4jLogger.info(msg: () -> String)  { if (isInfoEnabled)  info(msg())  }
internal inline fun Slf4jLogger.warn(msg: () -> String)  { if (isWarnEnabled)  warn(msg())  }
internal inline fun Slf4jLogger.warn(cause: Throwable, msg: () -> String) { if (isWarnEnabled) warn(msg(), cause) }
internal inline fun Slf4jLogger.error(cause: Throwable, msg: () -> String) { if (isErrorEnabled) error(msg(), cause) }

internal object Logging {
    inline fun <reified T : Any> of(): Slf4jLogger = LoggerFactory.getLogger(T::class.java)
}
