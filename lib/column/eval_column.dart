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

  Column<A> reindex(List<int> index) =>
      _force(new EvalColumn((int a) => f(index[a])), index.length);

  Column<A> force(int len) => _force(this, len);

  Column<A> _force(Column<A> col, int len) {
    var na = new MaskBuilder();
    var nm = new MaskBuilder();

    Column<A> loopAny(List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        var value = col.apply(i);
        if (value == NA) {
          na.add(i);
        } else if (value == NM) {
          nm.add(i);
        } else {
          xs[i] = value.get;
        }
        i += 1;
      }
      return new AnyColumn(xs, na.result(), nm.result());
    }

    Column<A> loopInt(Int32List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        var value = col.apply(i);
        if (value == NA) {
          na.add(i);
        } else if (value == NM) {
          nm.add(i);
        } else {
          try {
            xs[i] = value as int;
          } on CastError catch (_) {
            return loopAny(_copyToAnyArray(xs, i), i);
          }
        }
        i += 1;
      }
      return new IntColumn(xs, na.result(), nm.result()) as Column<A>;
    }

    Column<A> loopDouble(Float64List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        var value = col.apply(i);
        if (value == NA) {
          na.add(i);
        } else if (value == NM) {
          nm.add(i);
        } else {
          try {
            xs[i] = value as double;
          } on CastError catch (_) {
            return loopAny(_copyToAnyArray(xs, i), i);
          }
        }
        i += 1;
      }
      return new DoubleColumn(xs, na.result(), nm.result()) as Column<A>;
    }

    Column<A> loop(int i) {
      if (i < len) {
        var value = col.apply(i);
        if (value == NA) {
          na.add(i);
          return loop(i + 1);
        } else if (value == NM) {
          nm.add(i);
          return loop(i + 1);
        } else {
          var x = value;
          if (x is int) {
            var xs = new Int32List(len);
            xs[i] = x;
            return loopInt(xs, i + 1);
          } else if (x is num) {
            var xs = new Float64List(len);
            xs[i] = x;
            return loopDouble(xs, i + 1);
          } else {
            var xs = new List(len);
            xs[i] = x;
            return loopAny(xs, i + 1);
          }
        }
      } else {
        return new AnyColumn([], new Mask.empty(), nm.result());
      }
    }

    return loop(0);
  }

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
      return DenseColumn._zipMap(
          this.force(that.values.length) as DenseColumn, that, f);
    } else {
      return new EvalColumn((row) {
        var a = this.apply(row);
        var b = that.apply(row);

        if (a is Value && b is Value) {
          return new Value(f(a.get, b.get));
        } else if (a == NA || b == NA) {
          return NA;
        } else {
          return NM;
        }
      });
    }
  }
}
