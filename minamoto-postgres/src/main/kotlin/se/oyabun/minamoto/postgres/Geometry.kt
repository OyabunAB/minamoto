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

/** A point in 2-D space — Postgres `point` type. */
data class PgPoint(val x: Double, val y: Double)

/**
 * A rectangular box — Postgres `box` type.
 *
 * Postgres normalises the box so that [upperRight] always has the larger coordinates.
 */
data class PgBox(val upperRight: PgPoint, val lowerLeft: PgPoint)

/** A circle — Postgres `circle` type. */
data class PgCircle(val center: PgPoint, val radius: Double)

/**
 * An infinite line — Postgres `line` type.
 *
 * Represented as coefficients of the equation `Ax + By = C`.
 */
data class PgLine(val a: Double, val b: Double, val c: Double)

/** A line segment — Postgres `lseg` type. */
data class PgLseg(val start: PgPoint, val end: PgPoint)

/**
 * A geometric path — Postgres `path` type.
 *
 * [closed] is true when the path forms a closed polygon, false for an open path.
 */
data class PgPath(val closed: Boolean, val points: List<PgPoint>)

/** A polygon — Postgres `polygon` type. */
data class PgPolygon(val points: List<PgPoint>)
