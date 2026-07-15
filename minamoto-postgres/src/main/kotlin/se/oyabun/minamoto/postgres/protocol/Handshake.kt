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

import se.oyabun.aelv.await
import se.oyabun.aelv.discard
import se.oyabun.aelv.first
import se.oyabun.aelv.fold
import se.oyabun.aelv.rightOrThrow
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.postgres.Logging
import se.oyabun.minamoto.postgres.PostgresConnection
import se.oyabun.minamoto.postgres.protocol.Authentication
import se.oyabun.minamoto.postgres.protocol.BackendMessage.KeyData
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ErrorResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ReadyForQuery
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Bind
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.Parse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.PasswordMessage
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.SASLInitialResponse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.SASLResponse
import se.oyabun.minamoto.postgres.protocol.FrontendMessage.StartupMessage
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec
import javax.crypto.SecretKeyFactory

/**
 * Drives the PGwire startup sequence on an established [PostgresConnection].
 *
 * Supports MD5, cleartext, and SCRAM-SHA-256 authentication.
 * Throws [MinamotoException.AuthenticationFailed] on auth failure.
 * Throws [MinamotoException.InvalidState] on unexpected message types.
 */
internal suspend fun PostgresConnection.handshake(
    user:                            String,
    password:                        String,
    database:                        String,
    applicationName:                 String                = "minamoto",
    searchPath:                      List<String>          = emptyList(),
    timezone:                        String?               = null,
    statementTimeout:                kotlin.time.Duration? = null,
    lockTimeout:                     kotlin.time.Duration? = null,
    idleInTransactionSessionTimeout: kotlin.time.Duration? = null,
): KeyData {
    val log = Logging.of<PostgresConnection>()
    log.protocol.handshakeStarted(id)

    // Step 1 — send startup, wait for auth type
    val authMessage = exchange(
        messages  = listOf(StartupMessage(
            user                            = user,
            database                        = database,
            applicationName                 = applicationName,
            searchPath                      = searchPath,
            timezone                        = timezone,
            statementTimeout                = statementTimeout,
            lockTimeout                     = lockTimeout,
            idleInTransactionSessionTimeout = idleInTransactionSessionTimeout,
        )),
        takeUntil = { it is Authentication.Ok || it is Authentication.MD5Password ||
                      it is Authentication.CleartextPassword || it is Authentication.SASL ||
                      it is ErrorResponse },
    ).first().rightOrThrow()

    // Step 2 — respond to auth challenge
    when (authMessage) {
        is Authentication.Ok                -> log.protocol.authRequired(id, "none")
        is Authentication.CleartextPassword -> { log.protocol.authRequired(id, "cleartext"); sendPassword(password) }
        is Authentication.MD5Password       -> { log.protocol.authRequired(id, "md5"); sendPassword(md5Password(user, password, authMessage.salt)) }
        is Authentication.SASL              -> { log.protocol.authRequired(id, "scram-sha-256"); performScram(user, password, authMessage) }
        is ErrorResponse                   -> throw MinamotoException.AuthenticationFailed(authMessage.message)
        else -> throw MinamotoException.InvalidState("unexpected message during auth: $authMessage")
    }

    // Step 3 — consume server params until ReadyForQuery, extract KeyData
    data class HandshakeState(val backendKeyData: KeyData? = null)

    val state = exchange(
        messages  = emptyList(),
        takeUntil = { it is ReadyForQuery },
    ).fold(HandshakeState()) { acc, message ->
        when (message) {
            is KeyData -> acc.copy(backendKeyData = message)
            is ErrorResponse  -> throw MinamotoException.AuthenticationFailed(message.message)
            else              -> acc
        }
    }.await().rightOrThrow()

    log.protocol.handshakeComplete(id)
    return state.backendKeyData
        ?: throw MinamotoException.InvalidState("server did not send KeyData")
}

private suspend fun PostgresConnection.performScram(
    user:     String,
    password: String,
    message:  Authentication.SASL,
) {
    val mechanism = message.mechanisms.firstOrNull { it == SCRAM_SHA_256 }
        ?: throw MinamotoException.AuthenticationFailed("server does not support $SCRAM_SHA_256, offered: ${message.mechanisms}")

    // Client-first message
    val clientNonce     = generateNonce()
    val clientFirstBare = "n=$user,r=$clientNonce"
    val clientFirst     = "$GS2_HEADER$clientFirstBare"

    exchange(
        messages  = listOf(SASLInitialResponse(mechanism, clientFirst.toByteArray(Charsets.UTF_8))),
        takeUntil = { it is Authentication.SASLContinue || it is ErrorResponse },
    ).first().rightOrThrow().let { response ->
        if (response is ErrorResponse) throw MinamotoException.AuthenticationFailed(response.message)
        val serverFirst = (response as Authentication.SASLContinue).data.toString(Charsets.UTF_8)

        // Parse server-first: r=<nonce>,s=<salt>,i=<iterations>
        val params       = serverFirst.split(",").associate { it.substringBefore('=') to it.substringAfter('=') }
        val serverNonce  = params["r"] ?: throw MinamotoException.AuthenticationFailed("missing nonce in server-first")
        val salt         = Base64.getDecoder().decode(params["s"] ?: throw MinamotoException.AuthenticationFailed("missing salt"))
        val iterations   = (params["i"] ?: throw MinamotoException.AuthenticationFailed("missing iterations")).toInt()

        if (!serverNonce.startsWith(clientNonce))
            throw MinamotoException.AuthenticationFailed("server nonce does not start with client nonce")

        // Derive keys
        val saltedPassword = pbkdf2(password, salt, iterations)
        val clientKey      = hmacSha256(saltedPassword, CLIENT_KEY)
        val storedKey      = sha256(clientKey)
        val serverKey      = hmacSha256(saltedPassword, SERVER_KEY)

        // Client-final
        val channelBinding  = Base64.getEncoder().encodeToString(GS2_HEADER.toByteArray(Charsets.UTF_8))
        val clientFinalWithoutProof = "c=$channelBinding,r=$serverNonce"
        val authMessage2    = "$clientFirstBare,$serverFirst,$clientFinalWithoutProof"
        val clientSignature = hmacSha256(storedKey, authMessage2)
        val clientProof     = xorBytes(clientKey, clientSignature)
        val serverSignature = hmacSha256(serverKey, authMessage2)

        val clientFinal = "${clientFinalWithoutProof},p=${Base64.getEncoder().encodeToString(clientProof)}"

        exchange(
            messages  = listOf(SASLResponse(clientFinal.toByteArray(Charsets.UTF_8))),
            takeUntil = { it is Authentication.SASLFinal || it is Authentication.Ok || it is ErrorResponse },
        ).first().rightOrThrow().let { finalResponse ->
            when (finalResponse) {
                is ErrorResponse          -> throw MinamotoException.AuthenticationFailed(finalResponse.message)
                is Authentication.SASLFinal -> {
                    // Verify server signature
                    val serverParams = finalResponse.data.toString(Charsets.UTF_8)
                        .split(",").associate { it.substringBefore('=') to it.substringAfter('=') }
                    val serverSigReceived = Base64.getDecoder().decode(
                        serverParams["v"] ?: throw MinamotoException.AuthenticationFailed("missing server signature")
                    )
                    if (!serverSigReceived.contentEquals(serverSignature))
                        throw MinamotoException.AuthenticationFailed("server signature verification failed")
                }
                else -> { /* AuthenticationOk or other — fine */ }
            }
        }
    }
}

private suspend fun PostgresConnection.sendPassword(password: String) {
    exchange(
        messages  = listOf(PasswordMessage(password)),
        takeUntil = { it is Authentication.Ok || it is ErrorResponse },
    ).first().rightOrThrow().let { message ->
        if (message is ErrorResponse)
            throw MinamotoException.AuthenticationFailed(message.message)
    }
}

private const val SCRAM_SHA_256          = "SCRAM-SHA-256"
private const val HMAC_SHA256            = "HmacSHA256"
private const val PBKDF2_HMAC_SHA256     = "PBKDF2WithHmacSHA256"
private const val CLIENT_KEY             = "Client Key"
private const val SERVER_KEY             = "Server Key"
private const val GS2_HEADER            = "n,,"
private const val SCRAM_SHA_256_KEY_BITS = 256  // SHA-256 output size — fixed for SCRAM-SHA-256

private fun generateNonce(): String {
    val bytes = ByteArray(24)
    SecureRandom().nextBytes(bytes)
    return Base64.getEncoder().encodeToString(bytes)
}

private fun pbkdf2(password: String, salt: ByteArray, iterations: Int): ByteArray {
    val spec = PBEKeySpec(password.toCharArray(), salt, iterations, SCRAM_SHA_256_KEY_BITS)
    return SecretKeyFactory.getInstance(PBKDF2_HMAC_SHA256).generateSecret(spec).encoded
}

private fun hmacSha256(key: ByteArray, data: String): ByteArray =
    Mac.getInstance(HMAC_SHA256)
        .also { it.init(SecretKeySpec(key, HMAC_SHA256)) }
        .doFinal(data.toByteArray(Charsets.UTF_8))

private fun sha256(data: ByteArray): ByteArray =
    MessageDigest.getInstance("SHA-256").digest(data)

private fun xorBytes(a: ByteArray, b: ByteArray): ByteArray =
    ByteArray(a.size) { i -> (a[i].toInt() xor b[i].toInt()).toByte() }

private fun md5Password(user: String, password: String, salt: ByteArray): String {
    val md5   = MessageDigest.getInstance("MD5")
    val inner = md5.digest((password + user).toByteArray(Charsets.UTF_8)).toHex()
    md5.reset()
    val outer = md5.digest((inner + salt.toString(Charsets.ISO_8859_1)).toByteArray(Charsets.ISO_8859_1))
    return "md5" + outer.toHex()
}

private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
