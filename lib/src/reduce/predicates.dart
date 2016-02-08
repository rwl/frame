/*
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

part of frame.reduce;

/// A [Reducer] that exististentially quantifies a predicate
/// as a reduction over a collection of [Cell]s.
///
/// This reducer will return true upon encountering the first value
/// that is available and meaningful ''and'' applying the predicate
/// `p` to that value returns true. Otherwise, returns false.
///
/// This reducer is unusual in that it will ignore [NonValue]s, in
/// particular, it will not propogate [NM]. If the predicate can be
/// satisfied by a value that is available and meaningful elsewhere in
/// the collection, then this reduction should still return true.
///
/// This reducer will only traverse the entire collection if it never
/// encounters an available and meaningful value that satisfies the
/// predicate `p`.
///
/// ```
/// Series.empty<int, int>.reduce(new Exists<int>(i => true)) == Value(false)
///
/// Series(1 -> 1, 2 -> 2).reduce(new Exists<int>(i => i < 2)) == Value(true)
///
/// Series.fromCells<int, int>(1 -> NA, 2 -> 1).reduce(new Exists<int>(i => i < 2)) == Value(true)
/// Series.fromCells<int, int>(1 -> NM, 2 -> 1).reduce(new Exists<int>(i => i < 2)) == Value(true)
/// ```
///
/// This reducer will always return precisely `Value<bool>`,
/// rather than `Cell<bool>`. This in constrast to most reducers
/// that will also return [NonValue]s.
class Exists<A> implements Reducer<A, bool> {
  final Function p;
  Exists(bool _p(A a)) : p = _p;

  Value<bool> reduce(Column<A> column, List<int> indices, int start, int end) {
    column.forEach(start, end, (i) => indices[i], (_, a) {
      if (p(a)) {
        return new Value(true);
      }
    });
    return new Value(false);
  }
}

/// A [Reducer] that universally quantifies a predicate
/// as a reduction over a collection of [Cell]s.
///
/// This reducer will return false upon encountering the first value
/// that is not meaningful, or the first value that is available and
/// meaningful ''and'' applying the predicate `p` to that value
/// returns false. Otherwise, returns true.
///
/// This reducer does propogate [NM], in a sense, but the result is
/// `Value(false)` rather than `NM`. Unavailable values ([NA]) are
/// treated as the vaccuous case, so they will in count as a counter
/// example to the quantification.
///
/// This reducer will only traverse the entire collection if it never
/// encounters a not meaningful value or a meaningful value that does
/// not satisfy the predicate `p`.
///
/// ```
/// Series.empty<int, int>.reduce(new ForAll<int>(i => false)) == Value(true)
///
/// Series(1 -> 1, 2 -> 2).reduce(new ForAll<int>(i => i < 3)) == Value(true)
/// Series(1 -> 1, 2 -> 2).reduce(new ForAll<int>(i => i < 2)) == Value(false)
///
/// Series.fromCells<int, int>(1 -> NA)        .reduce(new ForAll<int>(i => false)) == Value(true)
/// Series.fromCells<int, int>(1 -> 1, 2 -> NM).reduce(new ForAll<int>(i => i < 2)) == Value(false)
/// ```
///
/// This reducer will always return precisely `Value<bool>`,
/// rather than `Cell<bool>`. This in constrast to most reducers
/// that will also return [NonValue]s.
class ForAll<A> extends Reducer<A, bool> {
  final Function p;
  ForAll(bool _p(A a)) : p = _p;

  Value<bool> reduce(Column<A> column, List<int> indices, int start, int end) {
    return new Value(column.forEach(start, end, (i) => indices[i], (_, a) {
      if (!p(a)) {
        return new Value(false);
      }
    }));
  }
}
