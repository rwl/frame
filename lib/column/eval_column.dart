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

typedef Cell EvalFunc(int a);

class EvalColumn<A extends Comparable> extends BoxedColumn<A> {
  final EvalFunc f;

  EvalColumn(this.f);

  @override
  Cell<A> apply(int row) => f(row);

  Column cellMap(Cell g(Cell<A> c)) => new EvalColumn((int a) => g(f(a)));

//  Column<A> reindex(List<int> index) =>
//      DenseColumn.force(new EvalColumn((int a) => f(index[a])), index.length);
//
//  Column<A> force(int len) => DenseColumn.force(this, len);

  Column<A> mask(Mask mask) =>
      new EvalColumn((int row) => mask[row] ? NA : f(row));

  Column<A> setNA(int naRow) =>
      new EvalColumn((int row) => row == naRow ? NA : f(row));

  Column<A> memoize([bool optimistic = false]) {
    if (optimistic) {
      return new OptimisticMemoizingColumn(f);
    } else {
      return new PessimisticMemoizingColumn(f);
    }
  }

  Column orElse(Column that) => new EvalColumn((int row) {
        var res = f(row);
        if (res == NM) {
          var res2 = that.apply(row);
          if (res2 == NA) {
            return NM;
          } else {
            return res2;
          }
        } else if (res == NA) {
          return that.apply(row);
        } else {
          return res;
        }
      });

  Column<A> shift(int n) => new EvalColumn((int row) => f(row - n));

  Column zipMap(Column that, dynamic f(A a, b)) {
    if (that is DenseColumn) {
      return DenseColumn.zipMap(this.force(that.values.length) as DenseColumn,
          that as DenseColumn, f);
    } else {
      return new EvalColumn((row) {
        var a = this.apply(row);
        var b = that.apply(row);

        if (a is Value && b is Value) {
          return new Value(f(a, b));
        } else if (a == NA || b == NA) {
          return NA;
        } else {
          return NM;
        }
      });
    }
  }
}
