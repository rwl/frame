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

/// A low level interface for implementing reductions.
///
/// TODO: All reducers should return `Cell<B>`.
///
/// [A] is the input type of the reducer, which is the value type of the
/// input [Column]. [B] is the ouput type of the reducer, which is the value
/// type of the output [Cell].
abstract class Reducer<A, B> {
  /// Reduce the given column of values to a cell using only the
  /// indexes in given array slice.
  ///
  /// [indices], [start], and [end] represent an array slice. The
  /// following must hold:
  ///
  ///  - `0 <= start`
  ///  - `end <= indices.length`
  ///  - `start <= end`
  ///
  /// and the slice is `[start:end)`, inclusive of [start] and
  /// exclusive of [end].
  ///
  /// Let `int i` where `start <= i` and `i < end`, then
  ///
  /// ```
  /// column(indices(i)) match {
  ///     case Value(v) => // 1.
  ///     case NA => // 2.
  ///     case NM => // 3.
  /// }
  /// ```
  ///
  /// 1. In the case of a [Value], the value should be included in the
  ///    reduction.
  /// 1. In the case of [NA], the reduction should skip.
  /// 1. In the case of [NM], the reduction should terminate with [NM].
  Cell<B> reduce(Column<A> column, List<int> indices, int start, int end);
}
