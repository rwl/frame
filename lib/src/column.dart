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

abstract class Column<A> {
  Column();

  // @unspecialized -- See TODO above.
  dynamic foldRow(int row, na, nm, f(a)) => /*macro*/ ColumnMacros
      .foldRowImpl();

  /**
   * Equivalent to calling `foreach(from, until, rows, true)(f)`.
   */
  bool foreach(int from, int until, int rows(int a), f(int a, A b)) =>
      ColumnMacros.foreachImpl();

  /**
   * Iterates from `from` until `until`, and for each value `i` in this range,
   * it retreives a row via `rows(i)`. If this row is `NM` and `abortOnNM` is
   * `true`, then iteration stops immediately and `false` is returned.
   * Otherwise, if the row is a value, it calls `f` with `i` and the value of
   * the row. If iteration terminates normally (ie. no [[NM]]s), then `true` is
   * returned.
   *
   * This is implemented as a macro and desugars into a while loop that access
   * the column using `apply` if it is a [[BoxedColumn]] and
   * `isValueAt`/`valueAt`/`nonValueAt` if it is an [[UnboxedColumn]]. It will
   * also inline `rows` and `f` if they are function literals.
   *
   * @param from  the value to start iterating at (inclusive)
   * @param until the value to stop iterating at (exclusive)
   * @param rows  the function used to retrieve the row for an iteration
   * @param f     the function to call at each value
   * @param abortOnNM terminate early if an `NM` is found
   * @return true if no NMs were found (or `abortOnNM` is false) and terminate completed successfully, false otherwise
   */
  bool foreach2(int from, int until, int rows(int a), bool abortOnNM,
          f(int a, A b)) =>
      ColumnMacros.foreachExtraImpl();

  /**
   * Returns the [[Cell]] at row `row`.
   */
  Cell<A> apply(int row);

  /**
   * Map all values of this `Column` using `f`. All [[NA]] and [[NM]] values
   * remain as they were.
   */
  Column<B> map(f(A a));

  /**
   * Map the values of this `Column` to a new [[Cell]]. All [[NA]] and [[NM]]
   * values remain the same.
   *
   * @param f function use to transform this column's values
   */
  Column<B> flatMap(Cell<B> f(A a));

  /**
   * Filters the values of this `Column` so that any value for which `p` is
   * true remains a value and all other values are turned into [[NA]]s.
   *
   * @param p predicate to filter this column's values with
   */
  Column<A> filter(bool p(A a));

  /**
   * Returns a column that will fallback to `that` for any row that is [[NA]],
   * or if the row is [[NM]] and the row in `that` is a [[Value]], then that
   * is returned, otherwise [[NM]] is returned.  That is, row `i` is defined as
   * `this(i) orElse that(i)`, though may be more efficient.
   *
   * To put the definition in more definite terms:
   *
   * {{{
   * Value(a) orElse Value(b) == Value(a)
   * Value(a) orElse       NA == Value(a)
   * Value(a) orElse       NM == Value(a)
   *       NA orElse Value(b) == Value(b)
   *       NA orElse       NA ==       NA
   *       NA orElse       NM ==       NM
   *       NM orElse Value(b) == Value(b)
   *       NM orElse       NM ==       NM
   *       NM orElse       NA ==       NM
   * }}}
   *
   * @param that the column to fallback on for NA values
   */
  Column orElse(Column that);

  /**
   * Returns a column whose `i`-th row maps to row `index(i)` in this column.
   * If `i &lt; 0` or `i &gt;= index.length` then the returned column returns
   * [[NA]]. This always forces all rows in `index` and the returned column is
   * *dense* and unboxed.
   */
  Column<A> reindex(List<int> index);

  /**
   * Returns a column which has had all rows between `0` and `len` (exclusive)
   * forced (evaluated) and stored in memory, while all rows outside of `0` and
   * `len` are set to [[NA]]. The returned column is *dense* and unboxed.
   *
   * @param len the upper bound of the range of values to force
   */
  Column<A> force(int len);

  /**
   * Returns a column with rows contained in `na` masked to [[NA]]s.
   *
   * @param na the rows to mask in the column
   */
  Column<A> mask(Mask na);

  /**
   * Returns a column with a single row forced to [[NA]] and all others
   * remaining the same. This is equivalent to, but possibly more efficient
   * than `col.mask(Mask(row))`.
   *
   * @param row the row that will be forced to [[NA]]
   */
  Column<A> setNA(int row);

  /**
   * Returns a copy of this column whose values will be memoized if they are
   * evaluated. That is, if `this` column is an *eval* column, then memoizing
   * it will ensure that, for each row, the value is only computed once,
   * regardless of the number of times it is accessed.
   *
   * By default, the memoization is always pessimistic (guaranteed at-most-once
   * evaluation). If `optimistic` is `true`, then the memoizing may use an
   * optimistic update strategy, which means a value *may* be evaluated more
   * than once if it accessed concurrently.
   *
   * For dense, empty, and previously-memoized columns, this just returns the
   * column itself.
   *
   * @param optimistic if true, memoized column may use optimistic updates
   */
  Column<A> memoize([bool optimistic = false]);

  /**
   * Shifts all values in the column up by `rows` rows. So,
   * `col.shift(n).apply(row) == col(row - n)`. If this is a dense column,
   * then it will only remain dense if `rows` is non-negative.
   */
  Column<A> shift(int rows);

  /**
   * For each `row` in the resulting column, this will return
   * `this(row).zipMap(that(row))`. Specifically, if `this(row)` or `that(row)`
   * is [[NA]], then the row is [[NA]], if both sides are values, then the row
   * is the result of applying `f`, otherwise the row is [[NM]].
   *
   * @param that the column to zip this column with
   * @param f    the function to use to combine 2 values to a single value
   */
  Column zipMap(Column that, f(A a, B b));

  @override
  String toString() {
    return range(5)
        .map((i) => apply(i).toString())
        .mkString("Column(", ", ", ", ...)");
  }

  static ColumnBuilder<A> newBuilder() => new ColumnBuilder<A>();

  /**
   * Construct a column whose `i`-th row is the `i`-th element in `cells`. All
   * other rows are [[NA]].
   */
  factory Column.fromCells(Iterable<Cell<A>> cells) {
    var bldr = newBuilder();
    cells.forEach((c) => bldr.add(c));
    return bldr.result();
  }

  /**
   * Returns a column which returns `Value(a)` for all rows.
   *
   * @note The `value` argument is strict.
   */
  static Column<A> value(value) {
    var cell = new Value(value);
    return new EvalColumn((c) => cell);
  }

  /**
   * Returns a column whose values are obtained using `get`. Each time a row is
   * accessed, `get` will be re-evaluated. To ensure values are evaluated
   * only once, you can [[memoize]] the column or use on of the *forcing*
   * methods, such as [[reindex]] or [[force]].
   */
  static Column eval(Cell get(int i)) => new EvalColumn(get);

  /**
   * Create a dense column from an array of values. A dense column can still
   * have empty values, [[NA]] and [[NM]], as specified with the `na` and `nm`
   * masks respectively. Dense columns are unboxed and only values that aren't
   * masked by `na` and `nm` will ever be returned (so they can be `null`,
   * `NaN`, etc.)
   *
   * The [[NM]] mask (`nm`) always takes precedence over the [[NA]] mask
   * (`na`).  If a row is outside of the range 0 until `values.length`, then if
   * `nm(row)` is true, [[NM]] will be returned, otherwise [[NA]] is returned.
   *
   * @param values the values of the column, rows correspond to indices
   * @param na     masked rows that will return [[NA]]
   * @param nm     masked rows that will return [[NM]]
   */
  static Column<A> dense(List<A> values,
      [Mask na = Mask.empty, Mask nm = Mask.empty]) {
//    switch (values) {
//    case (values: Array[Double]) => DoubleColumn(values, na, nm)
//    case (values: Array[Int]) => IntColumn(values, na, nm)
//    case (values: Array[Long]) => LongColumn(values, na, nm)
//    case _ => GenericColumn<A>(values, na, nm)
  }

  static Column<A> values(Iterable<A> values) {
    return new AnyColumn<A>(values.toArray(), Mask.empty(), Mask.empty());
  }

  /**
   * Returns a column that returns [[NM]] for any row in `nmValues` and [[NA]]
   * for all others. If all you need is a column that always returns [[NA]],
   * then use [[Empty]].
   */
  static Column<A> empty([Mask nmValues = Mask.empty]) {
    return new AnyColumn<A>(new List<Any>(0), Mask.empty(), nmValues);
  }

//  implicit def columnMonoid<A>: Monoid[Column<A>] =
//    new Monoid[Column<A>] {
//      def id: Column<A> = empty<A>()
//      def op(lhs: Column<A>, rhs: Column<A>): Column<A> =
//        lhs orElse rhs
//    }
}

abstract class BoxedColumn<A extends num> extends Column<A> {
  /**
   * Maps the cells of this [[Column]] using `f`. This method will always force
   * the column into an eval column and should be used with caution.
   */
  Column cellMap(Cell f(Cell<A> a));

  Column map(f(A a)) {
//    switch (cellMap) {
//    case Value(a):
//      return Value(f(a));
//    case (nonValue: NonValue) => nonValue
  }

  Column flatMap(Cell f(A a)) {
//    cellMap {
//    case Value(a) => f(a)
//    case (nonValue: NonValue) => nonValue
  }

  Column<A> filter(bool p(A a)) {
//    cellMap {
//    case Value(a) if p(a) => Value(a)
//    case Value(_) => NA
//    case nonValue => nonValue
  }
}

abstract class UnboxedColumn<A extends num> extends Column<A> {
  bool isValueAt(int row);
  NonValue nonValueAt(int row);
  A valueAt(int row);

  Cell<A> apply(int row) {
    if (isValueAt(row)) Value(valueAt(row));
    else nonValueAt(row);
  }
}
