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

import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class MessageEncoderTest {

    private val allocator = io.netty.buffer.UnpooledByteBufAllocator(false)

    private fun encode(message: FrontendMessage) =
        MessageEncoder.encode(message, allocator)

    @Test
    fun `startup message has correct length and protocol version`() {
        val buffer = encode(FrontendMessage.StartupMessage("alice", "mydb"))
        val length = buffer.readInt()
        assertEquals(buffer.readableBytes() + 4, length)
        val protocolVersion = buffer.readInt()
        assertEquals(196608, protocolVersion)
        buffer.release()
    }

    @Test
    fun `startup message encodes user and database`() {
        val buffer = encode(FrontendMessage.StartupMessage("alice", "mydb"))
        buffer.readInt()
        buffer.readInt()
        val content = ByteArray(buffer.readableBytes()).also { buffer.readBytes(it) }
        val text = String(content, Charsets.UTF_8)
        assert(text.contains("user")) { "missing user key" }
        assert(text.contains("alice")) { "missing user value" }
        assert(text.contains("database")) { "missing database key" }
        assert(text.contains("mydb")) { "missing database value" }
        buffer.release()
    }

    @Test
    fun `parse message has correct type byte`() {
        val buffer = encode(FrontendMessage.Parse("", "SELECT 1"))
        assertEquals('P'.code.toByte(), buffer.getByte(0))
        buffer.release()
    }

    @Test
    fun `sync message is 5 bytes`() {
        val buffer = encode(FrontendMessage.Sync)
        assertEquals(5, buffer.readableBytes())
        assertEquals('S'.code.toByte(), buffer.getByte(0))
        buffer.release()
    }

    @Test
    fun `terminate message is 5 bytes`() {
        val buffer = encode(FrontendMessage.Terminate)
        assertEquals(5, buffer.readableBytes())
        assertEquals('X'.code.toByte(), buffer.getByte(0))
        buffer.release()
    }

    @Test
    fun `execute message encodes portal name and max rows`() {
        val buffer = encode(FrontendMessage.Execute("myportal", 10))
        assertEquals('E'.code.toByte(), buffer.getByte(0))
        buffer.readByte()
        buffer.readInt()
        val portalName = StringBuilder()
        var byte = buffer.readByte()
        while (byte != 0.toByte()) { portalName.append(byte.toInt().toChar()); byte = buffer.readByte() }
        assertEquals("myportal", portalName.toString())
        assertEquals(10, buffer.readInt())
        buffer.release()
    }
}

class MessageDecoderTest {

    private fun buildMessage(type: Char, body: ByteArray): io.netty.buffer.ByteBuf {
        val buffer = Unpooled.buffer()
        buffer.writeByte(type.code)
        buffer.writeInt(body.size + 4)
        buffer.writeBytes(body)
        return buffer
    }

    private fun cstring(value: String) = value.toByteArray(Charsets.UTF_8) + byteArrayOf(0)

    @Test
    fun `decodes AuthenticationOk`() {
        val buffer  = buildMessage('R', byteArrayOf(0, 0, 0, 0))
        val message = MessageDecoder.decode(buffer)
        assertIs<BackendMessage.AuthenticationOk>(message)
        buffer.release()
    }

    @Test
    fun `decodes AuthenticationMD5Password with salt`() {
        val salt   = byteArrayOf(1, 2, 3, 4)
        val buffer = buildMessage('R', byteArrayOf(0, 0, 0, 5) + salt)
        val message = MessageDecoder.decode(buffer) as BackendMessage.AuthenticationMD5Password
        assertEquals(salt.toList(), message.salt.toList())
        buffer.release()
    }

    @Test
    fun `decodes ReadyForQuery idle`() {
        val buffer  = buildMessage('Z', byteArrayOf('I'.code.toByte()))
        val message = MessageDecoder.decode(buffer) as BackendMessage.ReadyForQuery
        assertIs<TransactionStatus.Idle>(message.transactionStatus)
        buffer.release()
    }

    @Test
    fun `decodes ReadyForQuery in transaction`() {
        val buffer  = buildMessage('Z', byteArrayOf('T'.code.toByte()))
        val message = MessageDecoder.decode(buffer) as BackendMessage.ReadyForQuery
        assertIs<TransactionStatus.InTransaction>(message.transactionStatus)
        buffer.release()
    }

    @Test
    fun `decodes CommandComplete`() {
        val buffer  = buildMessage('C', cstring("INSERT 0 1"))
        val message = MessageDecoder.decode(buffer) as BackendMessage.CommandComplete
        assertEquals("INSERT 0 1", message.tag)
        buffer.release()
    }

    @Test
    fun `decodes DataRow with two columns`() {
        val col1  = "hello".toByteArray(Charsets.UTF_8)
        val col2  = "world".toByteArray(Charsets.UTF_8)
        val body  = Unpooled.buffer()
        body.writeShort(2)
        body.writeInt(col1.size); body.writeBytes(col1)
        body.writeInt(col2.size); body.writeBytes(col2)
        val bodyBytes = ByteArray(body.readableBytes()).also { body.readBytes(it) }
        val buffer = buildMessage('D', bodyBytes)
        val message = MessageDecoder.decode(buffer) as BackendMessage.DataRow
        assertEquals(2, message.values.size)
        assertEquals("hello", message.values[0]!!.toString(Charsets.UTF_8))
        assertEquals("world", message.values[1]!!.toString(Charsets.UTF_8))
        buffer.release()
    }

    @Test
    fun `decodes DataRow with null column`() {
        val body = Unpooled.buffer()
        body.writeShort(1)
        body.writeInt(-1)
        val bodyBytes = ByteArray(body.readableBytes()).also { body.readBytes(it) }
        val buffer = buildMessage('D', bodyBytes)
        val message = MessageDecoder.decode(buffer) as BackendMessage.DataRow
        assertEquals(1, message.values.size)
        assertEquals(null, message.values[0])
        buffer.release()
    }

    @Test
    fun `decodes ErrorResponse fields`() {
        val body = Unpooled.buffer()
        body.writeByte('S'.code); body.writeBytes(cstring("ERROR"))
        body.writeByte('C'.code); body.writeBytes(cstring("23505"))
        body.writeByte('M'.code); body.writeBytes(cstring("duplicate key"))
        body.writeByte(0)
        val bodyBytes = ByteArray(body.readableBytes()).also { body.readBytes(it) }
        val buffer = buildMessage('E', bodyBytes)
        val message = MessageDecoder.decode(buffer) as BackendMessage.ErrorResponse
        assertEquals("ERROR", message.severity)
        assertEquals("23505", message.sqlState)
        assertEquals("duplicate key", message.message)
        buffer.release()
    }

    @Test
    fun `decodes ParseComplete`() {
        val buffer  = buildMessage('1', byteArrayOf())
        val message = MessageDecoder.decode(buffer)
        assertIs<BackendMessage.ParseComplete>(message)
        buffer.release()
    }

    @Test
    fun `decodes BindComplete`() {
        val buffer  = buildMessage('2', byteArrayOf())
        val message = MessageDecoder.decode(buffer)
        assertIs<BackendMessage.BindComplete>(message)
        buffer.release()
    }
}

class FramerTest {

    private val allocator = io.netty.buffer.UnpooledByteBufAllocator(false)

    private fun buildMessage(type: Char, body: ByteArray): ByteArray {
        val buffer = Unpooled.buffer()
        buffer.writeByte(type.code)
        buffer.writeInt(body.size + 4)
        buffer.writeBytes(body)
        val bytes = ByteArray(buffer.readableBytes()).also { buffer.readBytes(it) }
        buffer.release()
        return bytes
    }

    @Test
    fun `frames single complete message`() = kotlinx.coroutines.runBlocking {
        val messageBytes = buildMessage('Z', byteArrayOf('I'.code.toByte()))
        val input  = se.oyabun.aelv.Many.items(
            Unpooled.wrappedBuffer(messageBytes)
        )
        val framed = input.framed(allocator)

        se.oyabun.aelv.Verify.that(framed)
            .assertNext { buffer ->
                assertEquals('Z'.code.toByte(), buffer.getByte(0))
                buffer.release()
            }
            .completesNormally()
    }

    @Test
    fun `frames two messages from one chunk`() = kotlinx.coroutines.runBlocking {
        val message1 = buildMessage('1', byteArrayOf())
        val message2 = buildMessage('2', byteArrayOf())
        val combined = Unpooled.wrappedBuffer(message1 + message2)
        val input    = se.oyabun.aelv.Many.items(combined)
        val framed   = input.framed(allocator)

        se.oyabun.aelv.Verify.that(framed)
            .assertNext { buffer -> assertEquals('1'.code.toByte(), buffer.getByte(0)); buffer.release() }
            .assertNext { buffer -> assertEquals('2'.code.toByte(), buffer.getByte(0)); buffer.release() }
            .completesNormally()
    }

    @Test
    fun `frames message split across two chunks`() = kotlinx.coroutines.runBlocking {
        val messageBytes = buildMessage('Z', byteArrayOf('I'.code.toByte()))
        val firstHalf    = messageBytes.copyOf(3)
        val secondHalf   = messageBytes.copyOfRange(3, messageBytes.size)
        val input        = se.oyabun.aelv.Many.items(
            Unpooled.wrappedBuffer(firstHalf),
            Unpooled.wrappedBuffer(secondHalf),
        )
        val framed = input.framed(allocator)

        se.oyabun.aelv.Verify.that(framed)
            .assertNext { buffer ->
                assertEquals(messageBytes.size, buffer.readableBytes())
                buffer.release()
            }
            .completesNormally()
    }
}
