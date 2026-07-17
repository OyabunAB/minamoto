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
package se.oyabun.minamoto.postgres

/**
 * Supplies a password (or token) for each new connection.
 *
 * Called once per physical connection at creation time. Use this instead of a static
 * password string when credentials rotate — e.g. AWS IAM authentication tokens,
 * GCP service account tokens, or Vault-issued credentials.
 */
typealias CredentialSupplier = () -> String

/**
 * A single host in a [Hosts] list.
 */
data class Host(
    val hostname: String,
    val port:     Int = 5432,
)

/** Ordered list of hosts for [ConnectionConfig]. */
typealias Hosts = List<Host>

/**
 * Controls which host in a multi-host [ConnectionConfig] is selected for each new connection.
 *
 * [Any] connects to the first reachable host in the list — suitable for round-robin or
 * when all hosts are equivalent (e.g. PgBouncer instances).
 *
 * [Primary] connects only to the writable primary, determined by
 * `SELECT NOT pg_is_in_recovery()`. Retries each host until one reports it is not a replica.
 *
 * [Secondary] connects only to a read replica, determined by
 * `SELECT pg_is_in_recovery()`. Retries each host until one reports it is in recovery.
 * Throws [se.oyabun.minamoto.DatabaseException.ConnectionLost] if no secondary is found.
 */
sealed interface HostSelectionStrategy {
    data object Any       : HostSelectionStrategy
    data object Primary   : HostSelectionStrategy
    data object Secondary : HostSelectionStrategy
}
