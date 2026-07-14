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
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.*

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
        is StartupMessage  -> encodeStartup(message, allocator)
        is PasswordMessage -> encodeSingleMessage('p', allocator) { writeCString(message.password) }
        is Parse           -> encodeSingleMessage('P', allocator) {
            writeCString(message.statementName)
            writeCString(message.sql)
            writeShort(message.parameterOids.size)
            message.parameterOids.forEach { writeInt(it) }
        }
        is Bind            -> encodeSingleMessage('B', allocator) {
            writeCString(message.portalName)
            writeCString(message.statementName)
            writeShort(0) // all parameters use default (text) format
            writeShort(message.parameters.size)
            message.parameters.forEach { param ->
                if (param == null) {
                    writeInt(-1)
                } else {
                    writeInt(param.size)
                    writeBytes(param)
                }
            }
            writeShort(message.resultFormats.size)
            message.resultFormats.forEach { writeShort(it.toInt()) }
        }
        is Describe        -> encodeSingleMessage('D', allocator) {
            writeByte(if (message.target is DescribeTarget.Statement) 'S'.code else 'P'.code)
            writeCString(message.name)
        }
        is Execute         -> encodeSingleMessage('E', allocator) {
            writeCString(message.portalName)
            writeInt(message.maxRows)
        }
        is Close           -> encodeSingleMessage('C', allocator) {
            writeByte(if (message.target is DescribeTarget.Statement) 'S'.code else 'P'.code)
            writeCString(message.name)
        }
        is Sync            -> encodeSingleMessage('S', allocator) {}
        is Terminate       -> encodeSingleMessage('X', allocator) {}
        is CancelRequest   -> encodeCancelRequest(message, allocator)
        is SASLInitialResponse -> encodeSingleMessage('p', allocator) {
            writeCString(message.mechanism)
            writeInt(message.clientFirstMessage.size)
            writeBytes(message.clientFirstMessage)
        }
        is SASLResponse    -> encodeSingleMessage('p', allocator) {
            writeBytes(message.data)
        }
    }

    // ---------------------------------------------------------------------------
    // Startup — no message type byte, protocol version instead
    // ---------------------------------------------------------------------------

    private fun encodeStartup(message: StartupMessage, allocator: ByteBufAllocator): ByteBuf {
        val buf = allocator.buffer()
        val start = buf.writerIndex()
        buf.writeInt(0) // length placeholder
        buf.writeInt(PROTOCOL_VERSION)
        buf.writeCString("user")
        buf.writeCString(message.user)
        buf.writeCString("database")
        buf.writeCString(message.database)
        buf.writeCString("application_name")
        buf.writeCString(message.applicationName)
        buf.writeByte(0) // trailing null terminator
        buf.setInt(start, buf.writerIndex() - start)
        return buf
    }

    // ---------------------------------------------------------------------------
    // Cancel request — separate connection, no type byte, fixed format
    // ---------------------------------------------------------------------------

    private fun encodeCancelRequest(message: CancelRequest, allocator: ByteBufAllocator): ByteBuf {
        val buf = allocator.buffer(16)
        buf.writeInt(16)
        buf.writeInt(CANCEL_REQUEST_CODE)
        buf.writeInt(message.processId)
        buf.writeInt(message.secretKey)
        return buf
    }

    // ---------------------------------------------------------------------------
    // Standard message — type byte + int32 length + body
    // ---------------------------------------------------------------------------

    private fun encodeSingleMessage(
        type:      Char,
        allocator: ByteBufAllocator,
        body:      ByteBuf.() -> Unit,
    ): ByteBuf {
        val buf = allocator.buffer()
        buf.writeByte(type.code)
        val lengthIndex = buf.writerIndex()
        buf.writeInt(0) // length placeholder
        buf.body()
        buf.setInt(lengthIndex, buf.writerIndex() - lengthIndex)
        return buf
    }

    private fun ByteBuf.writeCString(value: String) {
        writeBytes(value.toByteArray(Charsets.UTF_8))
        writeByte(0)
    }

    private const val PROTOCOL_VERSION   = 196608  // 3.0
    private const val CANCEL_REQUEST_CODE = 80877102
}
