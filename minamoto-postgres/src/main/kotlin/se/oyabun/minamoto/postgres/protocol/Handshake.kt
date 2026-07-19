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

import se.oyabun.aelv.netty.ChannelBinding
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapNone
import se.oyabun.aelv.fold
import se.oyabun.aelv.map
import se.oyabun.aelv.or
import se.oyabun.aelv.then
import se.oyabun.minamoto.DatabaseException
import se.oyabun.minamoto.postgres.Logging
import se.oyabun.minamoto.postgres.PostgresConnection
import se.oyabun.minamoto.postgres.protocol.Authentication
import se.oyabun.minamoto.postgres.protocol.BackendMessage.KeyData
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ErrorResponse
import se.oyabun.minamoto.postgres.protocol.BackendMessage.ReadyForQuery
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
 * Returns [One<KeyData>] — the backend key data needed for [CancelRequest].
 */
internal fun PostgresConnection.handshake(
    user:                            String,
    password:                        String,
    database:                        String,
    applicationName:                 String                = "minamoto",
    searchPath:                      List<String>          = emptyList(),
    timezone:                        String?               = null,
    statementTimeout:                kotlin.time.Duration? = null,
    lockTimeout:                     kotlin.time.Duration? = null,
    idleInTransactionSessionTimeout: kotlin.time.Duration? = null,
    channelBinding:                  ChannelBinding        = ChannelBinding.None,
): One<KeyData> {
    val log = Logging.of<PostgresConnection>()
    log.protocol.handshakeStarted(id)

    return exchange(
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
    )
    .firstMaybe()
    .or { throw DatabaseException.InvalidState("no auth message received during handshake") }
    .flatMap { authMessage ->
        when (authMessage) {
            is Authentication.Ok                -> log.protocol.authRequired(id, "none")       { None.complete<BackendMessage>() }
            is Authentication.CleartextPassword -> log.protocol.authRequired(id, "cleartext")  { sendPassword(password) }
            is Authentication.MD5Password       -> log.protocol.authRequired(id, "md5")        { sendPassword(md5Password(user, password, authMessage.salt)) }
            is Authentication.SASL              -> log.protocol.authRequired(id, "scram")      { performScram(user, password, authMessage, channelBinding) }
            is ErrorResponse                    -> None.error<BackendMessage>(DatabaseException.AuthenticationFailed(authMessage.message))
            else -> None.error<BackendMessage>(DatabaseException.InvalidState("unexpected message during auth: $authMessage"))
        }
        .then {
            exchange(emptyList(), { it is ReadyForQuery })
                .fold(emptyList<KeyData>()) { acc: List<KeyData>, message: BackendMessage ->
                    when (message) {
                        is KeyData       -> listOf(message)
                        is ErrorResponse -> throw DatabaseException.AuthenticationFailed(message.message)
                        else             -> acc
                    }
                }
                .map { keyDataList: List<KeyData> ->
                    log.protocol.handshakeComplete(id)
                    keyDataList.firstOrNull() ?: throw DatabaseException.InvalidState("server did not send KeyData")
                }
        }
    }
}

private fun PostgresConnection.sendPassword(password: String): None<BackendMessage> =
    exchange(
        messages  = listOf(PasswordMessage(password)),
        takeUntil = { it is Authentication.Ok || it is ErrorResponse },
    ).firstMaybe()
     .or { throw DatabaseException.InvalidState("no response to password") }
     .flatMapNone { message ->
         if (message is ErrorResponse) None.error<BackendMessage>(DatabaseException.AuthenticationFailed(message.message))
         else None.complete<BackendMessage>()
     }

private fun PostgresConnection.performScram(
    user:           String,
    password:       String,
    message:        Authentication.SASL,
    channelBinding: ChannelBinding,
): None<BackendMessage> {
    val (mechanism, gs2Header, bindingDataBytes) = when {
        channelBinding is ChannelBinding.TlsServerEndPoint &&
        SCRAM_SHA_256_PLUS in message.mechanisms ->
            Triple(SCRAM_SHA_256_PLUS, "p=$CB_TYPE_TLS_SERVER_END_POINT,,", channelBinding.digest)
        SCRAM_SHA_256 in message.mechanisms ->
            Triple(SCRAM_SHA_256, "n,,", null)
        else -> return None.error(DatabaseException.AuthenticationFailed(
            "server does not support $SCRAM_SHA_256 or $SCRAM_SHA_256_PLUS, offered: ${message.mechanisms}"
        ))
    }

    val clientNonce     = generateNonce()
    val clientFirstBare = "n=$user,r=$clientNonce"
    val clientFirst     = "$gs2Header$clientFirstBare"

    return exchange(
        messages  = listOf(SASLInitialResponse(mechanism, clientFirst.toByteArray(Charsets.UTF_8))),
        takeUntil = { it is Authentication.SASLContinue || it is ErrorResponse },
    ).firstMaybe()
     .or { throw DatabaseException.InvalidState("no SASL continue response") }
     .flatMapNone { response ->
         if (response is ErrorResponse) return@flatMapNone None.error<BackendMessage>(DatabaseException.AuthenticationFailed(response.message))

         val serverFirst  = (response as Authentication.SASLContinue).data.toString(Charsets.UTF_8)
         val params       = serverFirst.split(",").associate { it.substringBefore('=') to it.substringAfter('=') }
         val serverNonce  = params["r"] ?: return@flatMapNone None.error<BackendMessage>(DatabaseException.AuthenticationFailed("missing nonce in server-first"))
         val salt         = Base64.getDecoder().decode(params["s"] ?: return@flatMapNone None.error<BackendMessage>(DatabaseException.AuthenticationFailed("missing salt")))
         val iterations   = (params["i"] ?: return@flatMapNone None.error<BackendMessage>(DatabaseException.AuthenticationFailed("missing iterations"))).toInt()

         if (!serverNonce.startsWith(clientNonce))
             return@flatMapNone None.error<BackendMessage>(DatabaseException.AuthenticationFailed("server nonce does not start with client nonce"))

         val saltedPassword = pbkdf2(password, salt, iterations)
         val clientKey      = hmacSha256(saltedPassword, CLIENT_KEY)
         val storedKey      = sha256(clientKey)
         val serverKey      = hmacSha256(saltedPassword, SERVER_KEY)

         val gs2HeaderBytes  = gs2Header.toByteArray(Charsets.UTF_8)
         val cbInput         = if (bindingDataBytes != null) gs2HeaderBytes + bindingDataBytes else gs2HeaderBytes
         val channelBindingB64       = Base64.getEncoder().encodeToString(cbInput)
         val clientFinalWithoutProof = "c=$channelBindingB64,r=$serverNonce"
         val authMessage2            = "$clientFirstBare,$serverFirst,$clientFinalWithoutProof"
         val clientSignature         = hmacSha256(storedKey, authMessage2)
         val clientProof             = xorBytes(clientKey, clientSignature)
         val serverSignature         = hmacSha256(serverKey, authMessage2)
         val clientFinal             = "${clientFinalWithoutProof},p=${Base64.getEncoder().encodeToString(clientProof)}"

         exchange(
             messages  = listOf(SASLResponse(clientFinal.toByteArray(Charsets.UTF_8))),
             takeUntil = { it is Authentication.SASLFinal || it is Authentication.Ok || it is ErrorResponse },
         ).firstMaybe()
          .or { throw DatabaseException.InvalidState("no SASL final response") }
          .flatMapNone { finalResponse ->
              when (finalResponse) {
                  is ErrorResponse -> None.error<BackendMessage>(DatabaseException.AuthenticationFailed(finalResponse.message))
                  is Authentication.SASLFinal -> {
                      val serverParams      = finalResponse.data.toString(Charsets.UTF_8).split(",").associate { it.substringBefore('=') to it.substringAfter('=') }
                      val serverSigReceived = Base64.getDecoder().decode(serverParams["v"] ?: return@flatMapNone None.error<BackendMessage>(DatabaseException.AuthenticationFailed("missing server signature")))
                      if (!serverSigReceived.contentEquals(serverSignature))
                          None.error<BackendMessage>(DatabaseException.AuthenticationFailed("server signature verification failed"))
                      else None.complete<BackendMessage>()
                  }
                  else -> None.complete<BackendMessage>()
              }
          }
     }
}

private const val SCRAM_SHA_256              = "SCRAM-SHA-256"
private const val SCRAM_SHA_256_PLUS         = "SCRAM-SHA-256-PLUS"
private const val CB_TYPE_TLS_SERVER_END_POINT = "tls-server-end-point"
private const val HMAC_SHA256                = "HmacSHA256"
private const val PBKDF2_HMAC_SHA256         = "PBKDF2WithHmacSHA256"
private const val CLIENT_KEY                 = "Client Key"
private const val SERVER_KEY                 = "Server Key"
private const val SCRAM_SHA_256_KEY_BITS     = 256

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

private fun sha256(data: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(data)

private fun xorBytes(a: ByteArray, b: ByteArray): ByteArray = ByteArray(a.size) { i -> (a[i].toInt() xor b[i].toInt()).toByte() }

private fun md5Password(user: String, password: String, salt: ByteArray): String {
    val md5   = MessageDigest.getInstance("MD5")
    val inner = md5.digest((password + user).toByteArray(Charsets.UTF_8)).toHex()
    md5.reset()
    val outer = md5.digest((inner + salt.toString(Charsets.ISO_8859_1)).toByteArray(Charsets.ISO_8859_1))
    return "md5" + outer.toHex()
}

private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
