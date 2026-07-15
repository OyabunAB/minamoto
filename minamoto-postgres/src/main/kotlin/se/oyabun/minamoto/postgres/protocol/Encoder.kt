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
package se.oyabun.minamoto.postgres.protocol

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import se.oyabun.minamoto.postgres.Parameter
import se.oyabun.minamoto.postgres.Parameters
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Bind
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.CancelRequest
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Close
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Describe
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Execute
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Parse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.PasswordMessage
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.SASLInitialResponse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.SASLResponse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.SSLRequest
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.StartupMessage
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Sync
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Terminate

/**
 * Encodes [FrontendMessage]s into PGwire-format [ByteBuf]s.
 *
 * All strings are written as null-terminated UTF-8.
 * All integers are big-endian, as required by the protocol.
 *
 * The caller is responsible for releasing the returned [ByteBuf] after writing it to the channel.
 */
internal object MessageEncoder {

    fun encode(message: FrontendMessage, allocator: ByteBufAllocator): ByteBuf = when (message) {
        is StartupMessage      -> encodeStartup(message, allocator)
        is PasswordMessage     -> encodeSingleMessage('p', allocator) { writeCString(message.password) }
        is Parse               -> encodeSingleMessage('P', allocator) {
            writeCString(message.statementName)
            writeCString(message.statement)
            writeShort(message.parameterOids.size)
            message.parameterOids.forEach { writeInt(it) }
        }
        is Bind                -> encodeSingleMessage('B', allocator) {
            writeCString(message.portalName)
            writeCString(message.statementName)
            val formats = message.parameters.map { param ->
                if (param is Parameter.Defined) param.format.wire.toInt() else 0
            }
            writeShort(formats.size)
            formats.forEach { writeShort(it) }
            writeShort(message.parameters.size)
            message.parameters.forEach { parameter ->
                when (parameter) {
                    is Parameter.Undefined -> writeInt(-1)
                    is Parameter.Defined   -> { writeInt(parameter.bytes.size); writeBytes(parameter.bytes) }
                }
            }
            writeShort(message.resultFormats.size)
            message.resultFormats.forEach { writeShort(it.toInt()) }
        }
        is Describe            -> encodeSingleMessage('D', allocator) {
            writeByte(if (message.target is DescribeTarget.Statement) 'S'.code else 'P'.code)
            writeCString(message.name)
        }
        is Execute             -> encodeSingleMessage('E', allocator) {
            writeCString(message.portalName)
            writeInt(message.maxRows)
        }
        is Close               -> encodeSingleMessage('C', allocator) {
            writeByte(if (message.target is DescribeTarget.Statement) 'S'.code else 'P'.code)
            writeCString(message.name)
        }
        is Sync                -> encodeSingleMessage('S', allocator) {}
        is Terminate           -> encodeSingleMessage('X', allocator) {}
        is CancelRequest       -> encodeCancelRequest(message, allocator)
        is SSLRequest          -> encodeSSLRequest(allocator)
        is SASLInitialResponse -> encodeSingleMessage('p', allocator) {
            writeCString(message.mechanism)
            writeInt(message.clientFirstMessage.size)
            writeBytes(message.clientFirstMessage)
        }
        is SASLResponse        -> encodeSingleMessage('p', allocator) {
            writeBytes(message.data)
        }
    }

    private fun encodeStartup(message: StartupMessage, allocator: ByteBufAllocator): ByteBuf {
        val buffer     = allocator.buffer()
        val startIndex = buffer.writerIndex()
        buffer.writeInt(0) // length placeholder
        buffer.writeInt(PROTOCOL_VERSION)
        buffer.writeCString("user");              buffer.writeCString(message.user)
        buffer.writeCString("database");          buffer.writeCString(message.database)
        buffer.writeCString("application_name");  buffer.writeCString(message.applicationName)
        if (message.searchPath.isNotEmpty()) {
            buffer.writeCString("search_path")
            buffer.writeCString(message.searchPath.joinToString(","))
        }
        message.timezone?.let {
            buffer.writeCString("timezone"); buffer.writeCString(it)
        }
        message.statementTimeout?.let {
            buffer.writeCString("statement_timeout")
            buffer.writeCString(it.inWholeMilliseconds.toString())
        }
        message.lockTimeout?.let {
            buffer.writeCString("lock_timeout")
            buffer.writeCString(it.inWholeMilliseconds.toString())
        }
        message.idleInTransactionSessionTimeout?.let {
            buffer.writeCString("idle_in_transaction_session_timeout")
            buffer.writeCString(it.inWholeMilliseconds.toString())
        }
        buffer.writeByte(0)
        buffer.setInt(startIndex, buffer.writerIndex() - startIndex)
        return buffer
    }

    private fun encodeSSLRequest(allocator: ByteBufAllocator): ByteBuf {
        val buffer = allocator.buffer(8)
        buffer.writeInt(8)
        buffer.writeInt(SSL_REQUEST_CODE)
        return buffer
    }

    private fun encodeCancelRequest(message: CancelRequest, allocator: ByteBufAllocator): ByteBuf {
        val buffer = allocator.buffer(16)
        buffer.writeInt(16)
        buffer.writeInt(CANCEL_REQUEST_CODE)
        buffer.writeInt(message.processId)
        buffer.writeInt(message.secretKey)
        return buffer
    }

    private fun encodeSingleMessage(
        type:      Char,
        allocator: ByteBufAllocator,
        body:      ByteBuf.() -> Unit,
    ): ByteBuf {
        val buffer      = allocator.buffer()
        buffer.writeByte(type.code)
        val lengthIndex = buffer.writerIndex()
        buffer.writeInt(0) // length placeholder
        buffer.body()
        buffer.setInt(lengthIndex, buffer.writerIndex() - lengthIndex)
        return buffer
    }

    private fun ByteBuf.writeCString(value: String) {
        writeBytes(value.toByteArray(Charsets.UTF_8))
        writeByte(0)
    }

    private const val PROTOCOL_VERSION    = 196608   // 3.0
    private const val CANCEL_REQUEST_CODE = 80877102
    private const val SSL_REQUEST_CODE    = 80877103
}
