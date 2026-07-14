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
import se.oyabun.aelv.Many
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.scan

/**
 * Splits a raw inbound byte stream into complete PGwire backend messages.
 *
 * PGwire framing: 1 byte type + 4 byte int32 length (includes itself, excludes type byte).
 *
 * Uses [scan] to accumulate incoming [ByteBuf] chunks into a [CompositeByteBuf], emitting
 * a list of complete messages whenever enough bytes are available. Downstream receives
 * each complete message buffer individually via [flatMap].
 *
 * Each emitted [ByteBuf] is a complete, self-contained message. The caller is responsible
 * for releasing it after decoding.
 */
internal fun Many<ByteBuf>.framed(allocator: ByteBufAllocator): Many<ByteBuf> =
    scan(allocator.compositeBuffer() as CompositeByteBuf) { acc: CompositeByteBuf, chunk: ByteBuf ->
        acc.addComponent(true, chunk)
        acc
    }
    .concatMap { acc: CompositeByteBuf ->
        val messages = mutableListOf<ByteBuf>()
        while (acc.readableBytes() >= 5) {
            val length = acc.getInt(acc.readerIndex() + 1)
            val total  = 1 + length
            if (acc.readableBytes() < total) break
            val message = allocator.buffer(total)
            acc.readBytes(message, total)
            messages.add(message)
        }
        Many.items(*messages.toTypedArray())
    }
