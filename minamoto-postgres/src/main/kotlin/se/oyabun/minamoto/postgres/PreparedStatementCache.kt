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

import se.oyabun.minamoto.postgres.Column
import java.util.concurrent.atomic.AtomicLong

/**
 * Per-connection LRU cache of named prepared statements.
 *
 * Named statements are scoped to a single PostgreSQL connection. The cache maps
 * SQL text to a server-side statement name (`s_<counter>`). When the cache is full,
 * the least-recently-used entry is evicted and the caller must send `Close(Statement, name)`
 * to release the server-side resource.
 *
 * Thread safety: all mutation is protected by the connection's `writeMutex` — callers
 * must hold that lock before calling any mutating method.
 *
 * @param maxSize Maximum number of entries. 0 disables the cache.
 */
internal class PreparedStatementCache(private val maxSize: Int) {

    data class Entry(
        val name:         String,
        val descriptions: List<Column.Description>,
    )

    private val counter = AtomicLong(0)

    private val lru: LinkedHashMap<String, Entry> = object : LinkedHashMap<String, Entry>(
        maxSize.coerceAtLeast(1),
        0.75f,
        true,
    ) {
        override fun removeEldestEntry(eldest: Map.Entry<String, Entry>): Boolean =
            size > maxSize
    }

    /** Returns the cached entry for [sql], or null if not cached or cache is disabled. */
    fun get(sql: String): Entry? {
        if (maxSize == 0) return null
        return lru[sql]
    }

    /**
     * Stores [entry] for [sql].
     *
     * Returns the evicted entry if the cache was full, so the caller can send
     * `Close(Statement, evicted.name)` to the server.
     */
    fun put(sql: String, entry: Entry): Entry? {
        if (maxSize == 0) return null
        val evictCandidate = if (lru.size >= maxSize) lru.entries.firstOrNull()?.value else null
        lru[sql] = entry
        return if (lru.size > maxSize) evictCandidate else null
    }

    /** Removes [sql] from the cache, returning the evicted entry or null. */
    fun evict(sql: String): Entry? {
        if (maxSize == 0) return null
        return lru.remove(sql)
    }

    /**
     * Generates the next unique statement name, or empty string when cache is disabled.
     *
     * An empty string instructs the caller to use the anonymous prepared statement
     * (PGwire `""`) which is discarded after each `Sync`.
     */
    fun nextName(): String = if (maxSize == 0) "" else "s_${counter.incrementAndGet()}"
}
