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
import io.netty.buffer.CompositeByteBuf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import se.oyabun.aelv.Many

/**
 * Splits a raw inbound byte stream into complete PGwire backend messages.
 *
 * PGwire message framing: 1 byte type + 4 byte length (includes itself, excludes type byte).
 * Accumulates incoming [ByteBuf]s until a complete message is available, then emits it.
 *
 * Each emitted [ByteBuf] is a complete, self-contained message. The caller is responsible
 * for releasing it after decoding.
 */
internal fun Many<ByteBuf>.framed(allocator: ByteBufAllocator): Many<ByteBuf> =
    Many.from(framedFlow(allocator))

private fun Many<ByteBuf>.framedFlow(allocator: ByteBufAllocator): Flow<ByteBuf> = flow {
    val accumulator: CompositeByteBuf = allocator.compositeBuffer()
    try {
        asFlow().collect { chunk ->
            accumulator.addComponent(true, chunk)
            while (true) {
                // Need at least 5 bytes to read type + length
                if (accumulator.readableBytes() < 5) break

                val length = accumulator.getInt(accumulator.readerIndex() + 1)
                val total  = 1 + length // type byte + length field + body

                if (accumulator.readableBytes() < total) break

                val message = allocator.buffer(total)
                accumulator.readBytes(message, total)
                emit(message)
            }
        }
    } finally {
        accumulator.release()
    }
}
