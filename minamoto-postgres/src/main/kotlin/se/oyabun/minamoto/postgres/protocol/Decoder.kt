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
import se.oyabun.minamoto.postgres.protocol.BackendMessage.*

/**
 * Decodes a single PGwire backend message from a [ByteBuf].
 *
 * Expects a complete message — type byte followed by int32 length followed by body.
 * Does not perform framing; use [MessageFramer] to split the inbound byte stream
 * into complete message buffers before passing them here.
 */
internal object MessageDecoder {

    fun decode(buf: ByteBuf): BackendMessage {
        val type   = buf.readByte().toInt().toChar()
        buf.readInt() // length — already framed, skip
        return when (type) {
            'R'  -> decodeAuthentication(buf)
            'S'  -> ParameterStatus(buf.readCString(), buf.readCString())
            'K'  -> BackendKeyData(buf.readInt(), buf.readInt())
            'Z'  -> ReadyForQuery(decodeTransactionStatus(buf.readByte().toInt().toChar()))
            't'  -> ParameterDescription(List(buf.readShort().toInt()) { buf.readInt() })
            'T'  -> decodeRowDescription(buf)
            'D'  -> decodeDataRow(buf)
            'C'  -> CommandComplete(buf.readCString())
            '1'  -> ParseComplete
            '2'  -> BindComplete
            '3'  -> CloseComplete
            's'  -> PortalSuspended
            'n'  -> NoData
            'I'  -> EmptyQueryResponse
            'E'  -> decodeErrorOrNotice(buf, notice = false)
            'N'  -> decodeErrorOrNotice(buf, notice = true)
            'A'  -> decodeNotification(buf)
            else -> throw MinamotoException.InvalidState("unknown backend message type: '$type'")
        }
    }

    // ---------------------------------------------------------------------------

    private fun decodeAuthentication(buf: ByteBuf): BackendMessage = when (val subtype = buf.readInt()) {
        0    -> AuthenticationOk
        3    -> AuthenticationCleartextPassword
        5    -> AuthenticationMD5Password(ByteArray(4).also { buf.readBytes(it) })
        10   -> {
            // AuthenticationSASL — list of mechanism names, null-terminated, double-null at end
            val mechanisms = mutableListOf<String>()
            while (buf.isReadable) {
                val name = buf.readCString()
                if (name.isEmpty()) break
                mechanisms.add(name)
            }
            AuthenticationSASL(mechanisms)
        }
        11   -> {
            val data = ByteArray(buf.readableBytes())
            buf.readBytes(data)
            AuthenticationSASLContinue(data)
        }
        12   -> {
            val data = ByteArray(buf.readableBytes())
            buf.readBytes(data)
            AuthenticationSASLFinal(data)
        }
        else -> throw MinamotoException.InvalidState("unsupported authentication type: $subtype")
    }

    private fun decodeTransactionStatus(char: Char): TransactionStatus = when (char) {
        'I'  -> TransactionStatus.Idle
        'T'  -> TransactionStatus.InTransaction
        'E'  -> TransactionStatus.FailedTransaction
        else -> throw MinamotoException.InvalidState("unknown transaction status: '$char'")
    }

    private fun decodeRowDescription(buf: ByteBuf): RowDescription {
        val count = buf.readShort().toInt()
        val columns = List(count) {
            ColumnDescription(
                name         = buf.readCString(),
                tableOid     = buf.readInt(),
                columnIndex  = buf.readShort(),
                typeOid      = buf.readInt(),
                typeSize     = buf.readShort(),
                typeModifier = buf.readInt(),
                formatCode   = buf.readShort(),
            )
        }
        return RowDescription(columns)
    }

    private fun decodeDataRow(buf: ByteBuf): DataRow {
        val count = buf.readShort().toInt()
        val values = List(count) {
            val length = buf.readInt()
            if (length == -1) null
            else ByteArray(length).also { buf.readBytes(it) }
        }
        return DataRow(values)
    }

    private fun decodeErrorOrNotice(buf: ByteBuf, notice: Boolean): BackendMessage {
        var severity = ""
        var sqlState = ""
        var message  = ""
        var detail   = ""
        var hint     = ""

        while (buf.isReadable) {
            val field = buf.readByte().toInt().toChar()
            if (field == '\u0000') break
            val value = buf.readCString()
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

    private fun decodeNotification(buf: ByteBuf): NotificationResponse =
        NotificationResponse(
            processId = buf.readInt(),
            channel   = buf.readCString(),
            payload   = buf.readCString(),
        )

    private fun ByteBuf.readCString(): String {
        val start = readerIndex()
        while (readByte() != 0.toByte()) { /* scan */ }
        val length = readerIndex() - start - 1
        return getCharSequence(start, length, Charsets.UTF_8).toString()
    }
}
