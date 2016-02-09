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

part of frame;

class Join {
  final bool leftOuter;
  final bool rightOuter;
  const Join._(this.leftOuter, this.rightOuter);

  static const Inner = const Join._(false, false);
  static const Left = const Join._(true, false);
  static const Right = const Join._(false, true);
  static const Outer = const Join._(true, true);
}

/// This implements a [Index.Cogrouper] that is suitable for generating
/// the indices necessary for joins on [Series] and [Frame].
class Joiner<K> extends Index.GenericJoin<K> {
  final Join join;
  Joiner(this.join);

  State cogroup(State state, List<K> lKeys, List<int> lIdx, int lStart,
      int lEnd, List<K> rKeys, List<int> rIdx, int rStart, int rEnd) {
    if (lEnd > lStart && rEnd > rStart) {
      var key = lKeys(lStart);
      for (var i = lStart; i < lEnd; i += 1) {
        var li = lIdx(i);
        for (var j = rStart; j < rEnd; j += 1) {
          state.add(key, li, rIdx(j));
        }
      }
    } else if (lEnd > lStart && join.leftOuter) {
      var key = lKeys(lStart);
      for (var i = lStart; i < lEnd; i += 1) {
        state.add(key, lIdx(i), Skip);
      }
    } else if (rEnd > rStart && join.rightOuter) {
      var key = rKeys(rStart);
      for (var i = rStart; i < rEnd; i += 1) {
        state.add(key, Skip, rIdx(i));
      }
    }

    reurn state;
  }
}
