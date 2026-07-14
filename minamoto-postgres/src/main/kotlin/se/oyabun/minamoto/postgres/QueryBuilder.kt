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
import se.oyabun.minamoto.Binding
import se.oyabun.minamoto.BoundCommand
import se.oyabun.minamoto.BoundEffect
import se.oyabun.minamoto.BoundQuery
import se.oyabun.minamoto.CommandBuilder
import se.oyabun.minamoto.EffectBuilder
import se.oyabun.minamoto.MinamotoException
import se.oyabun.minamoto.QueryBuilder
import se.oyabun.minamoto.Row
import se.oyabun.minamoto.postgres.protocol.executeCommand
import se.oyabun.minamoto.postgres.protocol.executeEffect
import se.oyabun.minamoto.postgres.protocol.executeQuery

/**
 * A query with named parameters, ready to be bound.
 *
 * Named parameters use `:name` syntax. [bind] rewrites the SQL to Postgres positional
 * parameters (`$1`, `$2`, ...) and encodes the values via the [CodecRegistry] held on
 * the connection.
 */
class PostgresQuery internal constructor(
    private val connection: PostgresConnection,
    private val statement:  String,
) : QueryBuilder {
    override fun bind(vararg bindings: Binding): PostgresBoundQuery {
        val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings.toList(), connection)
        return PostgresBoundQuery(connection, rewrittenSql, parameters)
    }
}

/**
 * A command (INSERT / UPDATE / DELETE / DDL) with named parameters, ready to be bound.
 */
class PostgresCommand internal constructor(
    private val connection: PostgresConnection,
    private val statement:  String,
) : CommandBuilder {
    override fun bind(vararg bindings: Binding): PostgresBoundCommand {
        val (rewrittenSql, parameters) = rewriteAndEncode(statement, bindings.toList(), connection)
        return PostgresBoundCommand(connection, rewrittenSql, parameters)
    }
}

class PostgresBoundQuery internal constructor(
    private val connection: PostgresConnection,
    private val statement:  String,
    private val parameters: Parameters,
) : BoundQuery {

    override fun multiple(): Many<Row> = connection.executeQuery(statement, parameters)

    override fun single(): One<Row> = connection.executeQuery(statement, parameters)
        .firstMaybe()
        .or { throw MinamotoException.QueryFailed(
            message  = "expected exactly one row but got none",
            sqlState = "02000",
            severity = "ERROR",
        )}

    override fun optional(): Maybe<Row> = connection.executeQuery(statement, parameters).firstMaybe()
}

class PostgresBoundCommand internal constructor(
    private val connection: PostgresConnection,
    private val statement:  String,
    private val parameters: Parameters,
) : BoundCommand {

    override fun count(): One<Long> = connection.executeCommand(statement, parameters)
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

/**
 * Encodes a named binding to a [Parameter].
 *
 * Passing `null` for [value] throws [MinamotoException.InvalidState] — use [Parameter.Undefined] to bind SQL NULL.
 */
private fun encodeBinding(name: String, value: Any?, connection: PostgresConnection): Parameter {
    if (value == null) throw MinamotoException.InvalidState(
        "no binding provided for parameter :$name — use Parameter.Undefined for SQL NULL"
    )
    if (value is Parameter.Undefined) return Parameter.Undefined
    val codec        = connection.registry.findForEncoding(value)
    val (bytes, fmt) = codec.encode(value)
    return Parameter.Defined(bytes, fmt)
}

internal fun PostgresConnection.query(statement: String): PostgresQuery = PostgresQuery(this, statement)
internal fun PostgresConnection.command(statement: String): PostgresCommand = PostgresCommand(this, statement)
internal fun PostgresConnection.effect(statement: String): PostgresEffect = PostgresEffect(this, statement)

class PostgresEffect internal constructor(
    private val connection: PostgresConnection,
    private val statement:  String,
) : EffectBuilder {
    override fun bind(vararg bindings: Binding): PostgresBoundEffect {
        val (rewrittenStatement, parameters) = rewriteAndEncode(statement, bindings.toList(), connection)
        return PostgresBoundEffect(connection, rewrittenStatement, parameters)
    }
}

class PostgresBoundEffect internal constructor(
    private val connection: PostgresConnection,
    private val statement:  String,
    private val parameters: Parameters,
) : BoundEffect {
    override fun execute() = connection.executeEffect(statement, parameters)
}
