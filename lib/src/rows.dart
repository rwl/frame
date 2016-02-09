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

abstract class Rows<K, A> extends AxisSelectionLike<K, A, Rows> {
  Cols<K, A> toCols() {
    if (this is AllRows) {
      return new AllCols(e);
    } else if (this is PickRows) {
      return new PickCols(keys, e);
    } else if (this is WrappedRows) {
      return WrappedCols(sel);
    }
  }
}

//object Rows extends AxisSelectionCompanion[Rows] {
class AllRows<K, A> extends Rows<K, A> with AllAxisSelection<K, A> {
  AllRows(RowExtractor<A, K, Variable> extractor);
}
//  object All extends AllCompanion

class PickRows<K, S extends Size, A> extends Rows<K, A>
    with PickAxisSelection<K, S, A> {
  PickRows(List<K> keys, RowExtractor<A, K, S> extractor);
}
//  object Pick extends PickCompanion

class WrappedRows<K, A> extends Rows<K, A> with WrappedAxisSelection<K, A> {
  WrappedRows(AxisSelection<K, A> sel);
}
//  object Wrapped extends WrappedCompanion
//}
