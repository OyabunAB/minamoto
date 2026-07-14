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
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.postgres.Column
import se.oyabun.minamoto.postgres.protocol.Authentication
import se.oyabun.minamoto.postgres.protocol.BackendMessage.KeyData
import se.oyabun.minamoto.postgres.protocol.BackendMessage.BindComplete
import se.oyabun.minamoto.postgres.protocol.BackendMessage.CloseComplete
import se.oyabun.minamoto.postgres.protocol.BackendMessage.CommandComplete
import se.oyabun.minamoto.postgres.protocol.BackendMessage.DataRow
import se.oyabun.minamoto.postgres.protocol.BackendMessage.EmptyQueryResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ErrorResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NoData
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NoticeResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.NotificationResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ParameterDescription
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ParameterStatus
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ParseComplete
import se.oyabun.minamoto.postgres.protocol.BackendMessage.PortalSuspended
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ReadyForQuery
import se.oyabun.minamoto.postgres.protocol.BackendMessage.RowDescription

/**
 * Decodes a single PGwire backend message from a [ByteBuf].
 *
 * Expects a complete message — type byte followed by int32 length followed by body.
 * Does not perform framing; use [MessageFramer] to split the inbound byte stream
 * into complete message buffers before passing them here.
 */
internal object MessageDecoder {

    fun decode(buffer: ByteBuf): BackendMessage {
        val type = buffer.readByte().toInt().toChar()
        buffer.readInt() // length — already framed, skip
        return when (type) {
            'R'  -> decodeAuthentication(buffer)
            'S'  -> ParameterStatus(buffer.readCString(), buffer.readCString())
            'K'  -> KeyData(buffer.readInt(), buffer.readInt())
            'Z'  -> ReadyForQuery(decodeTransactionStatus(buffer.readByte().toInt().toChar()))
            't'  -> ParameterDescription(List(buffer.readShort().toInt()) { buffer.readInt() })
            'T'  -> decodeRowDescription(buffer)
            'D'  -> decodeDataRow(buffer)
            'C'  -> CommandComplete(buffer.readCString())
            '1'  -> ParseComplete
            '2'  -> BindComplete
            '3'  -> CloseComplete
            's'  -> PortalSuspended
            'n'  -> NoData
            'I'  -> EmptyQueryResponse
            'E'  -> decodeErrorOrNotice(buffer, notice = false)
            'N'  -> decodeErrorOrNotice(buffer, notice = true)
            'A'  -> decodeNotification(buffer)
            else -> throw MinamotoException.InvalidState("unknown backend message type: '$type'")
        }
    }

    private fun decodeAuthentication(buffer: ByteBuf): BackendMessage = when (val subtype = buffer.readInt()) {
        0    -> Authentication.Ok
        3    -> Authentication.CleartextPassword
        5    -> Authentication.MD5Password(ByteArray(4).also { buffer.readBytes(it) })
        10   -> {
            val mechanisms = mutableListOf<String>()
            while (buffer.isReadable) {
                val name = buffer.readCString()
                if (name.isEmpty()) break
                mechanisms.add(name)
            }
            Authentication.SASL(mechanisms)
        }
        11   -> {
            val data = ByteArray(buffer.readableBytes())
            buffer.readBytes(data)
            Authentication.SASLContinue(data)
        }
        12   -> {
            val data = ByteArray(buffer.readableBytes())
            buffer.readBytes(data)
            Authentication.SASLFinal(data)
        }
        else -> throw MinamotoException.InvalidState("unsupported authentication type: $subtype")
    }

    private fun decodeTransactionStatus(char: Char): TransactionStatus = when (char) {
        'I'  -> TransactionStatus.Idle
        'T'  -> TransactionStatus.InTransaction
        'E'  -> TransactionStatus.FailedTransaction
        else -> throw MinamotoException.InvalidState("unknown transaction status: '$char'")
    }

    private fun decodeRowDescription(buffer: ByteBuf): RowDescription {
        val columnCount = buffer.readShort().toInt()
        val columns     = List(columnCount) {
            Column.Description(
                name         = buffer.readCString(),
                tableOid     = buffer.readInt(),
                columnIndex  = buffer.readShort(),
                typeOid      = buffer.readInt(),
                typeSize     = buffer.readShort(),
                typeModifier = buffer.readInt(),
                formatCode   = buffer.readShort(),
            )
        }
        return RowDescription(columns)
    }

    private fun decodeDataRow(buffer: ByteBuf): DataRow {
        val columnCount = buffer.readShort().toInt()
        val values      = List(columnCount) {
            val length = buffer.readInt()
            if (length == -1) null
            else ByteArray(length).also { buffer.readBytes(it) }
        }
        return DataRow(values)
    }

    private fun decodeErrorOrNotice(buffer: ByteBuf, notice: Boolean): BackendMessage {
        var severity = ""
        var sqlState = ""
        var message  = ""
        var detail   = ""
        var hint     = ""

        while (buffer.isReadable) {
            val field = buffer.readByte().toInt().toChar()
            if (field == '\u0000') break
            val value = buffer.readCString()
            when (field) {
                'S' -> severity = value
                'C' -> sqlState = value
                'M' -> message  = value
                'D' -> detail   = value
                'H' -> hint     = value
            }
        }

        return if (notice) NoticeResponse(severity, sqlState, message, detail, hint)
        else               ErrorResponse(severity, sqlState, message, detail, hint)
    }

    private fun decodeNotification(buffer: ByteBuf): NotificationResponse =
        NotificationResponse(
            processId = buffer.readInt(),
            channel   = buffer.readCString(),
            payload   = buffer.readCString(),
        )

    private fun ByteBuf.readCString(): String {
        val startIndex = readerIndex()
        while (readByte() != 0.toByte()) { /* scan to null terminator */ }
        val length = readerIndex() - startIndex - 1
        return getCharSequence(startIndex, length, Charsets.UTF_8).toString()
    }
}
