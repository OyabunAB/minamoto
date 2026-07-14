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

import se.oyabun.aelv.Many
import se.oyabun.aelv.Maybe
import se.oyabun.aelv.One
import se.oyabun.aelv.firstMaybe
import se.oyabun.aelv.or
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.postgres.protocol.executeCommand
import se.oyabun.minamoto.postgres.protocol.executeQuery

/**
 * A query with named parameters, ready to be bound.
 *
 * Named parameters use `:name` syntax. [bind] rewrites the SQL to Postgres positional
 * parameters (`$1`, `$2`, ...) and encodes the values via the [CodecRegistry] held on
 * the connection.
 */
class PreparedQuery internal constructor(
    private val connection: PostgresConnection,
    private val sql:        String,
) {
    fun bind(vararg bindings: Binding): BoundQuery {
        val (rewrittenSql, parameters) = rewriteAndEncode(sql, bindings.toList(), connection)
        return BoundQuery(connection, rewrittenSql, parameters)
    }

    /** Execute with no parameters. */
    fun multiple(): Many<Row> = bind().multiple()
    fun single(): One<Row>    = bind().single()
    fun optional(): Maybe<Row> = bind().optional()
}

/**
 * A command (INSERT / UPDATE / DELETE / DDL) with named parameters, ready to be bound.
 */
class PreparedCommand internal constructor(
    private val connection: PostgresConnection,
    private val sql:        String,
) {
    fun bind(vararg bindings: Binding): BoundCommand {
        val (rewrittenSql, parameters) = rewriteAndEncode(sql, bindings.toList(), connection)
        return BoundCommand(connection, rewrittenSql, parameters)
    }

    fun single(): One<Long> = BoundCommand(connection, sql, emptyList()).single()
}

/**
 * A query with encoded parameters, ready to execute.
 *
 * Use [multiple] to stream all rows, [single] to assert exactly one row,
 * or [optional] when zero or one row is expected.
 */
class BoundQuery internal constructor(
    private val connection: PostgresConnection,
    private val sql:        String,
    private val parameters: Parameters,
) {
    /** Stream all result rows. */
    fun multiple(): Many<Row> = connection.executeQuery(sql, parameters)

    /**
     * Return exactly one row. Signals [MinamotoException.QueryFailed] (SQLSTATE 02000)
     * if the query returns zero rows, or if it returns more than one.
     */
    fun single(): One<Row> = connection.executeQuery(sql, parameters)
        .firstMaybe()
        .or { throw MinamotoException.QueryFailed(
            message  = "expected exactly one row but got none",
            sqlState = "02000",
            severity = "ERROR",
        )}

    /** Return the first row if present, empty [Maybe] if the query returns no rows. */
    fun optional(): Maybe<Row> = connection.executeQuery(sql, parameters).firstMaybe()
}

/**
 * A command with encoded parameters, ready to execute.
 */
class BoundCommand internal constructor(
    private val connection: PostgresConnection,
    private val sql:        String,
    private val parameters: Parameters,
) {
    /** Execute and return the number of rows affected. */
    fun single(): One<Long> = connection.executeCommand(sql, parameters)
}

/**
 * Rewrites `:name` parameters to Postgres `$n` positional parameters and encodes values.
 *
 * Returns the rewritten SQL and the [Parameter] list in positional order.
 * Skips rewriting inside single-quoted literals, dollar-quoted strings, and comments.
 */
private fun rewriteAndEncode(
    sql:        String,
    bindings:   List<Binding>,
    connection: PostgresConnection,
): Pair<String, Parameters> {
    val bindingMap  = bindings.toMap()
    val nameToIndex = mutableMapOf<String, Int>()
    val parameters  = mutableListOf<Parameter>()
    val result      = StringBuilder()
    var index       = 0

    while (index < sql.length) {
        when {
            sql[index] == '\'' -> {
                result.append(sql[index++])
                while (index < sql.length) {
                    val char = sql[index++]
                    result.append(char)
                    if (char == '\'' && (index >= sql.length || sql[index] != '\'')) break
                }
            }

            sql[index] == '$' && index + 1 < sql.length && (sql[index + 1] == '$' || sql[index + 1].isLetter()) -> {
                val tagEnd   = sql.indexOf('$', index + 1)
                if (tagEnd == -1) { result.append(sql[index++]); continue }
                val tag      = sql.substring(index, tagEnd + 1)
                val closeTag = sql.indexOf(tag, tagEnd + 1)
                if (closeTag == -1) { result.append(sql[index++]); continue }
                result.append(sql.substring(index, closeTag + tag.length))
                index = closeTag + tag.length
            }

            sql[index] == '-' && index + 1 < sql.length && sql[index + 1] == '-' -> {
                val eol = sql.indexOf('\n', index).let { if (it == -1) sql.length else it + 1 }
                result.append(sql.substring(index, eol))
                index = eol
            }

            sql[index] == '/' && index + 1 < sql.length && sql[index + 1] == '*' -> {
                val close = sql.indexOf("*/", index + 2).let { if (it == -1) sql.length else it + 2 }
                result.append(sql.substring(index, close))
                index = close
            }

            sql[index] == ':' && index + 1 < sql.length && sql[index + 1] == ':' -> {
                result.append("::")
                index += 2
            }

            sql[index] == ':' && index + 1 < sql.length && sql[index + 1].isLetter() -> {
                val nameStart = index + 1
                var nameEnd   = nameStart
                while (nameEnd < sql.length && (sql[nameEnd].isLetterOrDigit() || sql[nameEnd] == '_')) nameEnd++
                val name      = sql.substring(nameStart, nameEnd)
                val position  = nameToIndex.getOrPut(name) {
                    parameters.add(encodeBinding(name, bindingMap[name], connection))
                    parameters.size
                }
                result.append("\$$position")
                index = nameEnd
            }

            else -> result.append(sql[index++])
        }
    }

    return Pair(result.toString(), parameters)
}

private fun encodeBinding(name: String, value: Any?, connection: PostgresConnection): Parameter {
    if (value == null) throw MinamotoException.InvalidState(
        "no binding provided for parameter :$name — use Parameter.Undefined for SQL NULL"
    )
    if (value is Parameter.Undefined) return Parameter.Undefined
    val codec        = connection.registry.findForEncoding(value)
    val (bytes, fmt) = codec.encode(value)
    return Parameter.Defined(bytes, fmt)
}

internal fun PostgresConnection.query(sql: String): PreparedQuery = PreparedQuery(this, sql)
internal fun PostgresConnection.command(sql: String): PreparedCommand = PreparedCommand(this, sql)
