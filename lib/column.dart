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

abstract class Column<A extends Comparable> {
  Column();

  dynamic foldRow(int row, na, nm, f(a));

  /// Iterates from `from` until `until`, and for each value `i` in this range,
  /// it retreives a row via `rows(i)`. If this row is `NM` and `abortOnNM` is
  /// `true`, then iteration stops immediately and `false` is returned.
  /// Otherwise, if the row is a value, it calls `f` with `i` and the value of
  /// the row. If iteration terminates normally (ie. no [NM]s), then `true` is
  /// returned.
  bool forEach(int from, int until, int rows(int a), f(int a, A b),
      [bool abortOnNM = true]);

  /// Returns the [Cell] at row `row`.
  Cell<A> apply(int row);

  /// Map all values of this `Column` using `f`. All [NA] and [NM] values
  /// remain as they were.
  Column map(f(A a));

  /// Map the values of this `Column` to a new [Cell]. All [NA] and [NM]
  /// values remain the same.
  Column flatMap(Cell f(A a));

  /// Filters the values of this `Column` so that any value for which `p` is
  /// true remains a value and all other values are turned into [NA]s.
  Column<A> filter(bool p(A a));

  /// Returns a column that will fallback to `that` for any row that is [NA],
  /// or if the row is [NM] and the row in `that` is a [Value], then that
  /// is returned, otherwise [NM] is returned.  That is, row `i` is defined
  /// as `this(i).orElse(that(i))`, though may be more efficient.
  ///
  /// To put the definition in more definite terms:
  ///
  /// ```
  /// Value(a).orElse(Value(b) == Value(a))
  /// Value(a).orElse(      NA == Value(a))
  /// Value(a).orElse(      NM == Value(a))
  ///       NA.orElse(Value(b) == Value(b))
  ///       NA.orElse(      NA ==       NA)
  ///       NA.orElse(      NM ==       NM)
  ///       NM.orElse(Value(b) == Value(b))
  ///       NM.orElse(      NM ==       NM)
  ///       NM.orElse(      NA ==       NM)
  /// ```
  Column orElse(Column that);

  /// Returns a column whose `i`-th row maps to row `index(i)` in this column.
  /// If `i < 0` or `i >= index.length` then the returned column returns
  /// [NA]. This always forces all rows in `index` and the returned column is
  /// *dense* and unboxed.
  Column<A> reindex(List<int> index);

  /// Returns a column which has had all rows between `0` and `len` (exclusive)
  /// forced (evaluated) and stored in memory, while all rows outside of `0` and
  /// `len` are set to [NA]. The returned column is *dense* and unboxed.
  Column<A> force(int len);

  /// Returns a column with rows contained in `na` masked to [NA]s.
  Column<A> mask(Mask na);

  /// Returns a column with a single row forced to [NA] and all others
  /// remaining the same. This is equivalent to, but possibly more efficient
  /// than `col.mask(Mask(row))`.
  Column<A> setNA(int row);

  /// Returns a copy of this column whose values will be memoized if they are
  /// evaluated. That is, if `this` column is an *eval* column, then memoizing
  /// it will ensure that, for each row, the value is only computed once,
  /// regardless of the number of times it is accessed.
  ///
  /// By default, the memoization is always pessimistic (guaranteed at-most-once
  /// evaluation). If `optimistic` is `true`, then the memoizing may use an
  /// optimistic update strategy, which means a value *may* be evaluated more
  /// than once if it accessed concurrently.
  ///
  /// For dense, empty, and previously-memoized columns, this just returns the
  /// column itself.
  Column<A> memoize([bool optimistic = false]);

  /// Shifts all values in the column up by `rows` rows. So,
  /// `col.shift(n).apply(row) == col(row - n)`. If this is a dense column,
  /// then it will only remain dense if `rows` is non-negative.
  Column<A> shift(int rows);

  /// For each `row` in the resulting column, this will return
  /// `this(row).zipMap(that(row))`. Specifically, if `this(row)` or `that(row)`
  /// is [NA], then the row is [NA], if both sides are values, then the row
  /// is the result of applying `f`, otherwise the row is [NM].
  Column zipMap(Column that, f(A a, b));

  @override
  String toString() {
    return "Column(" +
        range(5).map((i) => apply(i).toString()).join(", ") +
        ", ...)";
  }

  /// Construct a column whose `i`-th row is the `i`-th element in `cells`. All
  /// other rows are [NA].
  factory Column.fromCells(Iterable<Cell<A>> cells) {
    var bldr = new ColumnBuilder();
    cells.forEach((c) => bldr.add(c));
    return bldr.result();
  }

  /// Returns a column which returns `Value(a)` for all rows.
  factory Column.value(value) {
    var cell = new Value(value);
    return new EvalColumn((c) => cell);
  }

  /// Returns a column whose values are obtained using `get`. Each time a row
  /// is accessed, `get` will be re-evaluated. To ensure values are evaluated
  /// only once, you can [memoize] the column or use on of the *forcing*
  /// methods, such as [reindex] or [force].
  factory Column.eval(Cell get(int i)) => new EvalColumn(get);

  /// Create a dense column from an array of values. A dense column can still
  /// have empty values, [NA] and [NM], as specified with the `na` and `nm`
  /// masks respectively. Dense columns are unboxed and only values that aren't
  /// masked by `na` and `nm` will ever be returned (so they can be `null`,
  /// `NaN`, etc.)
  ///
  /// The [NM] mask (`nm`) always takes precedence over the [NA] mask
  /// (`na`).  If a row is outside of the range 0 until `values.length`, then if
  /// `nm(row)` is true, [NM] will be returned, otherwise [NA] is returned.
  factory Column.dense(List<A> values, [Mask na, Mask nm]) {
    if (na == null) {
      na = new Mask.empty();
    }
    if (nm == null) {
      nm = new Mask.empty();
    }
//    if (type == double) {
//      return new DoubleColumn(values, na, nm);
//    } else if (type == int) {
//      return new IntColumn(values, na, nm);
//    } else {
    return new GenericColumn<A>(values, na, nm);
//    }
  }

  factory Column.fromValues(Iterable<A> values) {
    return new /*AnyColumn*/ GenericColumn<A>(
        values.toList(), new Mask.empty(), new Mask.empty());
  }

  /// Returns a column that returns [NM] for any row in `nmValues` and [NA]
  /// for all others. If all you need is a column that always returns [NA],
  /// then use [Empty].
  factory Column.empty([Mask nmValues]) {
    if (nmValues == null) {
      nmValues = new Mask.empty();
    }
    return new /*AnyColumn*/ GenericColumn<A>([], new Mask.empty(), nmValues);
  }
}

class ColumnMonoid<A extends Comparable> implements Monoid<Column<A>> {
  final Column<A> lhs;
  ColumnMonoid(this.lhs);

  Column<A> empty() => new Column<A>.empty();

  Column<A> merge(Column<A> other) => lhs.orElse(other);
}

abstract class BoxedColumn<A extends num> extends Column<A> {
  dynamic foldRow(int row, na, nm, f(a)) {
    var c = apply(row);
    if (c == NA) {
      return na();
    } else if (c == NM) {
      return nm();
    } else {
      return f(c);
    }
  }

  bool forEach(int from, int until, int rows(int a), f(int a, A b),
      [bool abortOnNM = true]) {
    bool nm = false;
    var i = from;
    while (i < until && !nm) {
      var row = rows(i);
      var v = apply(row);
      if (v is Value) {
        f(i, v.get);
      } else if (v == NM) {
        nm = abortOnNM;
      }
      i += 1;
    }

    return !nm;
  }

  /// Maps the cells of this [Column] using `f`. This method will always
  /// force the column into an eval column and should be used with caution.
  Column cellMap(Cell f(Cell<A> a));

  Column map(f(A a)) => cellMap((c) => c is Value ? new Value(f(c.get)) : c);

  Column flatMap(Cell f(A a)) => cellMap((c) => c is Value ? f(c.get) : c);

  Column<A> filter(bool p(A a)) {
    return cellMap((c) {
      if (c is Value) {
        return p(c.get) ? new Value(c.get) : NA;
      } else if (c is NonValue) {
        return c;
      }
    });
  }
}

abstract class UnboxedColumn<A extends Comparable> extends Column<A> {
  bool isValueAt(int row);
  NonValue nonValueAt(int row);
  A valueAt(int row);

  dynamic foldRow(int row, na, nm, f(a)) {
    var r = row;
    if (isValueAt(r)) {
      return f(valueAt(r));
    } else {
      var nv = nonValueAt(r);
      if (nv == NA) {
        return na();
      } else if (nv == NM) {
        return nm();
      } else {
        throw nv;
      }
    }
  }

  bool forEach(int from, int until, int rows(int a), f(int a, A b),
      [bool abortOnNM = true]) {
    bool nm = false;
    var i = from;
    while (i < until && !nm) {
      var row = rows(i);
      if (isValueAt(row)) {
        f(i, valueAt(row));
      } else if (nonValueAt(row) == NM) {
        nm = abortOnNM;
      }
      i += 1;
    }

    return !nm;
  }

  Cell<A> apply(int row) {
    if (isValueAt(row)) {
      return new Value(valueAt(row));
    } else {
      return nonValueAt(row);
    }
  }
}

class ColumnBuilder<A extends Comparable> {
  var i = 0;
  List<A> values = [];
  MaskBuilder na = new MaskBuilder();
  MaskBuilder nm = new MaskBuilder();

  void addValue(A a) {
    values.add(a);
    i += 1;
  }

  void addNA() {
    na.add(i);
    values.add(null);
    i += 1;
  }

  void addNM() {
    nm.add(i);
    values.add(null);
    i += 1;
  }

  void add(Cell<A> cell) {
    if (cell is Value) {
      addValue(cell.get);
    } else if (cell == NA) {
      addNA();
    } else if (cell == NM) {
      addNM();
    }
  }

  Column<A> result() => new GenericColumn(values, na.result(), nm.result());

  void clear() {
    i = 0;
    values.clear();
    na.clear();
    nm.clear();
  }
}
