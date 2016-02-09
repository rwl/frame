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

class Merge {
  final bool outer;
  const Merge._(this.outer);

  static const Inner = const Merge._(false); // intersection
  static const Outer = const Merge._(true); // union
}

/// This implements a [Index.Cogrouper] that is suitable for generating
/// the indices necessary for merges and appends on [Series] and [Frame].
class Merger<K> extends Index.GenericJoin<K> {
  final Merge merge;
  Merger(this.merge);

  State cogroup(State state, List<K> lKeys, List<int> lIdx, int lStart,
      int lEnd, List<K> rKeys, List<int> rIdx, int rStart, int rEnd) {
    if (lEnd > lStart && rEnd > rStart) {
      val key = lKeys(lStart);

      var rPosition = rStart;
      var lPosition = lStart;

      // When doing an outer join, we iterate over the left index and right index till *both* are
      // exhausted
      if (merge.outer) {
        while (lPosition < lEnd || rPosition < rEnd) {
          // If either the left index become exhausted, start returning `Skip` elements to indicate
          // there is no match for the side
          var li = (lPosition >= lEnd) ? Skip : lIdx(lPosition);
          var ri = (rPosition >= rEnd) ? Skip : rIdx(rPosition);
          lPosition += 1;
          rPosition += 1;
          state.add(key, li, ri);
        }
      } else {
        while (lPosition < lEnd && rPosition < rEnd) {
          state.add(key, lIdx(lPosition), rIdx(rPosition));
          lPosition += 1;
          rPosition += 1;
        }
      }
    } else if (merge.outer) {
      if (lEnd > lStart) {
        val key = lKeys(lStart);
        for (var i = lStart; i < lEnd; i += 1) {
          state.add(key, lIdx(i), Skip);
        }
      } else if (rEnd > rStart) {
        var key = rKeys(rStart);
        for (var i = rStart; i < rEnd; i += 1) {
          state.add(key, Skip, rIdx(i));
        }
      }
    }

    return state;
  }
}
