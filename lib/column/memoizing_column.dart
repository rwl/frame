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

abstract class MemoizingColumn<A extends Comparable> extends BoxedColumn<A> {
  EvalColumn<A> _eval = new EvalColumn((int a) => apply(a));

  Column cellMap(Cell f(Cell<A> c)) => _eval.cellMap(f);

  Column<A> reindex(List<int> index) => _eval.reindex(index);

  Column<A> force(int len) => _eval.force(len);

  Column<A> mask(Mask mask) => _eval.mask(mask);

  Column<A> setNA(int naRow) => _eval.setNA(naRow);

  Column<A> memoize([bool optimistic = false]) => this;

  Column orElse(Column that) => _eval.orElse(that);

  Column<A> shift(int n) => _eval.shift(n);

  Column zipMap(Column that, dynamic f(A a, b)) => _eval.zipMap(that, f);
}

class OptimisticMemoizingColumn<A extends Comparable>
    extends MemoizingColumn<A> {
  final Map<int, Cell<A>> _cached = {};

  final EvalFunc _get;

  OptimisticMemoizingColumn(this._get);

  Cell<A> apply(int row) {
    if (!_cached.containsKey(row)) {
      _cached[row] = _get(row);
    }
    return _cached[row];
  }
}

class PessimisticMemoizingColumn<A extends Comparable>
    extends MemoizingColumn<A> {
  final Map<int, Box> _cached = {};

  final EvalFunc _get;

  PessimisticMemoizingColumn(this._get);

  Cell<A> apply(int row) {
    if (!_cached.containsKey(row)) {
      _cached[row] = new Box<A>(row, _get);
    }
    return _cached[row].cell;
  }
}

/// A Box let's us do the double-checked locking per-value, rather
/// than having to lock the entire cache for the update.
class Box<A extends Comparable> {
  final int _row;
  final EvalFunc _get;

  Box(this._row, this._get);

  Cell<A> _cell = null;

  Cell<A> get cell {
    if (_cell == null) {
      _cell = _get(_row);
    }
    return _cell;
  }
}
