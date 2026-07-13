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

import io.netty.buffer.ByteBufAllocator
import io.netty.channel.Channel
import kotlinx.coroutines.flow.first
import se.oyabun.aelv.netty.inbound
import se.oyabun.aelv.netty.write
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.postgres.protocol.BackendMessage.*
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.*
import java.security.MessageDigest

/**
 * Drives the PGwire startup sequence on an established TCP [Channel].
 *
 * Sends [StartupMessage], handles authentication ([AuthenticationOk],
 * [AuthenticationMD5Password], [AuthenticationCleartextPassword]), consumes
 * [ParameterStatus] and [BackendKeyData], and returns when [ReadyForQuery] is received.
 *
 * Throws [MinamotoException.AuthenticationFailed] on auth failure.
 * Throws [MinamotoException.InvalidState] on unexpected message types.
 */
internal suspend fun Channel.handshake(
    user:      String,
    password:  String,
    database:  String,
    allocator: ByteBufAllocator = alloc(),
): HandshakeResult {
    val messages = inbound().framed(allocator)
    var backendKeyData: BackendKeyData? = null

    write(MessageEncoder.encode(StartupMessage(user, database), allocator)).await()

    messages.asFlow().first { buf ->
        try {
            when (val message = MessageDecoder.decode(buf)) {
                is AuthenticationOk                -> false
                is AuthenticationCleartextPassword -> {
                    write(MessageEncoder.encode(PasswordMessage(password), allocator)).await()
                    false
                }
                is AuthenticationMD5Password       -> {
                    val hashed = md5Password(user, password, message.salt)
                    write(MessageEncoder.encode(PasswordMessage(hashed), allocator)).await()
                    false
                }
                is ParameterStatus                 -> false
                is BackendKeyData                  -> { backendKeyData = message; false }
                is ReadyForQuery                   -> true
                is ErrorResponse                   -> throw MinamotoException.AuthenticationFailed(message.message)
                else -> throw MinamotoException.InvalidState("unexpected message during handshake: $message")
            }
        } finally {
            buf.release()
        }
    }

    return HandshakeResult(
        backendKeyData = backendKeyData
            ?: throw MinamotoException.InvalidState("server did not send BackendKeyData"),
    )
}

/**
 * The result of a completed handshake.
 *
 * [backendKeyData] carries the process ID and secret key needed to send cancel requests.
 */
data class HandshakeResult(
    val backendKeyData: BackendKeyData,
)

/**
 * Computes the MD5 password hash as required by PGwire.
 *
 * Format: "md5" + md5(md5(password + user) + salt)
 */
private fun md5Password(user: String, password: String, salt: ByteArray): String {
    val md5 = MessageDigest.getInstance("MD5")
    val inner = md5.digest((password + user).toByteArray(Charsets.UTF_8))
    val innerHex = inner.toHex()
    md5.reset()
    val outer = md5.digest((innerHex + salt.toString(Charsets.ISO_8859_1)).toByteArray(Charsets.ISO_8859_1))
    return "md5" + outer.toHex()
}

private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
