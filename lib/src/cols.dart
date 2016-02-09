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

abstract class Cols<K, A> extends AxisSelectionLike<K, A, Cols> {
  Rows<K, A> toRows() {
    if (this is AllCols) {
      return AllRows(e);
    } else if (this is PickCols) {
      return PickRows(keys, e);
    } else if (this is WrappedCols) {
      return Rows.Wrapped(sel);
    }
  }
}

//object Cols extends AxisSelectionCompanion[Cols] {
class AllCols<K, A> extends Cols<K, A> with AllAxisSelection<K, A> {
  AllCols(RowExtractor<A, K, Variable> extractor);
}
//object All extends AllCompanion

class PickCols<K, S extends Size, A> extends Cols<K, A>
    with PickAxisSelection<K, S, A> {
  PickCols(List<K> keys, RowExtractor<A, K, S> extractor);
}
//object Pick extends PickCompanion

class WrappedCols<K, A> extends Cols<K, A> with WrappedAxisSelection<K, A> {
  WrappedCols(AxisSelection<K, A> sel);
}
//object Wrapped extends WrappedCompanion
//}
