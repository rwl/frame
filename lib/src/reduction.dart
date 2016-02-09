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

class Reduction<K, A, B> extends Index.Grouper<K> {
  final Column<A> column;
  final Reducer<A, B> reducer;

  Reduction(this.column, this.reducer);

  init() => new State();

  State group(
      State state, List<K> keys, List<int> indices, int start, int end) {
    state.add(keys(start), reducer.reduce(column, indices, start, end));
    return state;
  }
}

class State<K, B> {
  var keys = <K>[];
  var values = <Cell<B>>[];

  add(K key, Cell<B> value) {
    keys.add(key);
    values.add(value);
  }

  Tuple2<List<K>, List<Cell<B>>> result() =>
      new Tuple2(keys.result(), values.result());
}
