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

class Series<K, V> {
  final Index<K> index;
  final Column<V> column;

  Series(this.index, this.column) {
    _classTag = index.classTag;
    _order = index.order;
  }

  var _classTag;
  var _order;

  int get size => index.size;

  /** Returns this series as a collection of key/value pairs. */
  Iterable<Tuple2<K, Cell<V>>> to() {
//  [CC[_](implicit cbf: CanBuildFrom[Nothing, (K, Cell<V>), CC[(K, Cell<V>)]): CC[(K, Cell<V>)] = {
//    val bldr = cbf()
//    bldr.sizeHint(size)
//    iterator.foreach { bldr += _ }
//    bldr.result()
  }

  /// Returns an iterator over the key-cell pairs of the series.
  ///
  /// If the series is known to be dense, or the non values
  /// can ignored, then one should use [denseIterator] instead.
  Iterator<Tuple<K, Cell<V>>> iterator() {
//    return index.iterator map { case (k, row) => k -> column(row)
  }

  /// Returns an iterator over the key-value pairs of the series.
  ///
  /// This iterator assumes that the series is dense, so it will
  /// skip over any non value cells if, in fact, the series is sparse.
  ///
  /// The [Iterator] returned by this method is related to
  /// the [Iterator] returned by [iterator]
  /// ```
  /// series.iterator.collect { case (k, Value(v)) => k -> v} == series.denseIterator
  /// ```
  /// however, this method uses a more efficient access pattern
  /// to the underlying data.
  Iterator<Tuple2<K, V>> denseIterator() {
//    new Iterator[(K, V)] {
//      private var i = -1
//      private var pair: (K, V) = _
//      private def findNext(): Unit = {
//        i += 1
//        while (i < index.size) {
//          column.foldRow(index.indexAt(i))((), (), { v =>
//            pair = (index.keyAt(i), v)
//            return ()
//          })
//          i += 1
//        }
//      }
//
//      findNext()
//
//      def hasNext: Boolean = i < index.size
//      def next(): (K, V) = {
//        val result = pair
//        findNext()
//        result
//      }
//    }
  }

  /// Applies a function [f] to all key-cell pairs of the series.
  ///
  /// The series is traversed in index order.
  void forEach(f(Tuple2<K, Cell<V>> a)) {
    for (var i = 0; i < index.size; i += 1) {
      f(index.keyAt(i), column(index.indexAt(i)));
    }
  }

  /// Applies a function [f] to all key-value pairs of the series.
  ///
  /// The series is traversed in index order.
  ///
  /// This method assumes that the series is dense, so it will skip
  /// over any non value cells if, in fact, the series is sparse.
  void forEachDense(f(Tuple2<K, V> a)) {
    for (var i = 0; i < index.size; i += 1) {
      var row = index.indexAt(i);
//      column.foldRow(row)((), (), f(index.keyAt(i), _));
    }
  }

  /// Applies a function [f] to all keys of the series.
  ///
  /// The series is traversed in index order.
  void forEachKeys(f(K a)) {
    for (var i = 0; i < index.size; i += 1) {
      f(index.keyAt(i));
    }
  }

  /// Applies a function [f] to all cells of the series.
  ///
  /// The series is traversed in index order.
  void forEachCells(f(Cell<V> a)) {
    for (var i = 0; i < index.size; i += 1) {
      f(column(index.indexAt(i)));
    }
  }

  /// Applies a function [f] to all values of the series.
  ///
  /// The series is traversed in index order.
  ///
  /// This method assumes that the series is dense, so it will skip
  /// over any non value cells if, in fact, the series is sparse.
  Unit forEachValues(f(V a)) {
    for (var i = 0; i < index.size; i += 1) {
      val row = index.indexAt(i);
//      column.foldRow(row)((), (), f);
    }
  }

  /// Returns the keys of this series as a vector in index order.
  List<K> get keys {
    var builder = [];
    for (var i = 0; i < index.size; i += 1) {
      builder.add(index.keyAt(i));
    }
    return builder;
  }

  /// Return the cells of this series as a vector in index order.
  ///
  /// The series may be sparse, so the vector contains [Cell]s
  /// rather than just plain values.
  List<Cell<V>> get cells {
    var builder = <Cell<V>>[];
    for (var i = 0; i < index.size; i += 1) {
      builder.add(column(index.indexAt(i)));
    }
    return builder;
  }

  /// Returns the values of this series as a vector in index order.
  ///
  /// If the series is dense, this returns the values directly,
  /// rather than wrapped in [Cell]. If the series is infact sparse,
  /// the [NonValue]s are ignored.
  ///
  /// The [List] returned by this method is related to
  /// the [List] returned by [values]
  /// ```
  /// series.cells.collect { case Value(v) => v } == series.values
  /// ```
  /// however, this method uses a more efficient access pattern
  /// to the underlying data.
  List<V> values() {
    var builder = <V>[];
    forEachValues((v) => builder.add(v));
    return builder;
  }

  K keyAt(int i) => index.keyAt(i);

  Cell<V> cellAt(int i) => column(index.indexAt(i));

  V valueAt(int i) {
    var row = index.indexAt(i);
    V error() => throw new NoSuchElementException("No value at row $row");
//    column.foldRow(row, error, error, (v) => v: V);
  }

  Cell<V> apply(K key) => index.get(key).map((i) => column(i)).getOrElse(NA);

  /// Returns all cells with with key of [key].
  Vector<Cell<V>> getCells(K key) {
//    return index.getAll(key).map { case (_, row) => column(row) } (collection.breakOut);
  }

  /// Returns all values with with key of [key].
  Vector<V> getValues(K key) {
    var bldr = <V>[];
//    index.getAll(key).foreach((_, row) {
//      column.foldRow(row)((), (), (a) => bldr.add(a));
//    });
    return bldr;
  }

  /// Returns `true` if at least 1 value exists in this series. A series
  /// with only `NA`s and/or `NM`s will return `false`.
  bool get hasValues {
    var i = 0;
    var seenValue = false;
    while (i < index.size && !seenValue) {
      val row = index.indexAt(i);
      seenValue = column.foldRow(row, false, false, (_) => true);
      i += 1;
    }
    return seenValue;
  }

  /// Returns `true` if the series is logically empty. That is, just in case
  /// none of its rows contain a value. Presently, both [NA]s and [NM]s are
  /// not considered to be values.
  bool get isEmpty => !hasValues;

  /// Combines 2 series together using the functions provided to handle each
  /// case. If a value exists in both `this` and [that], then [both] is used
  /// to combine the value to a new one, otherwise either [left] or [right]
  /// are used, unless both are missing, then the missing value is returned
  /// ([NA] is both are [NA] and [NM] otherwise).
  Series<K, X> combine(
      Series<K, W> that, X left(V a), X right(W a), X both(V a, W b)) {
    var merger = new Merger<K>(Merge.Outer);
    var cg = Index.cogroup(this.index, that.index)(merger).result();
    var keys = cg.v1, lIndices = cg.v2, rIndices = cg.v3;
    var lCol = this.column;
    var rCol = that.column;

    Builder<X> bldr = Column.newBuilder();
    for (var i = 0; i < lIndices.length; i += 1) {
      var l = lIndices(i);
      var r = rIndices(i);
      lCol.foldRow(l)(
          rCol.foldRow(r)(
              bldr.addNA(), bldr.addNM(), (w) => bldr.addValue(right(w))),
          bldr.addNM(),
          (v) => rCol.foldRow(r)(bldr.addValue(left(v)), bldr.addNM(),
              (w) => bldr.addValue(both(v, w))));
    }

    return Series(Index.ordered(keys), bldr.result());
  }

  /// Merges 2 series together using a semigroup to append values.
  Series<K, VV> merge(Series<K, VV> that) {
//    return combine(that, (v) => v, (v) => v, (v, w) => (v: VV) |+| (w: VV))
  }

  /// Concatenates [that] onto the end of `this` [Series].
  Series<K, VV> concat(Series<K, VV> that) {
    Builder<K, VV> bldr = Series.newUnorderedBuilder();
    this.foreach(bldr.append);
    that.foreach(bldr.append);
    bldr.result();
  }

  /// Merges 2 series together, taking the first non-NA or NM value.
  Series<K, VV> orElse(Series<K, VV> that) {
    var merger = new Merger<K>(Merge.Outer);
    var cg = Index.cogroup(this.index, that.index)(merger).result();
    var keys = cg.v1, lIndices = cg.v2, rIndices = cg.v3;
    var lCol = this.column.reindex(lIndices);
    var rCol = that.column.reindex(rIndices);
    return new Series(Index.ordered(keys), lCol.orElse(rCol));
  }

  /// Perform an inner join with [that] and group the values in tuples.
  ///
  /// Equivalent to calling `lhs.zipMap(rhs)((_, _))`.
  Series<K, Tuple2<V, W>> zip(Series<K, W> that) =>
      zipMap(that, (a, b) => new Tuple2(a, b));

  /// Performs an inner join on this [Series] with [that]. Each pair of
  /// values for a matching key is passed to [f].
  Series<K, X> zipMap(Series<K, W> that, X f(Tuple2<V, W> a)) {
    var joiner = new Joiner<K>(Join.Inner);
    var cg = Index.cogroup(this.index, that.index, joiner).result();
    var keys = cg.v1, lIndices = cg.v2, rIndices = cg.v3;

    // TODO: Add zipMap method to Column, then reindex/zipMap instead!
    var lCol = this.column.reindex(lIndices);
    var rCol = that.column.reindex(rIndices);
    var col = lCol.zipMap(rCol, f);
    return new Series(Index.ordered(keys), col);
  }

  /// Sort this series by index keys and return it. The sort should be
  /// stable, so the relative order within a key will remain the same.
  Series<K, V> sorted() => new Series(index.sorted, column);

  /// Convert this Series to a single column [Frame].
//  Frame<K, C> toFrame(C col, ClassTag<V> tt) => ColOrientedFrame(index, Series(col -> TypedColumn(column)));

  Frame<K, int> toFrame(ClassTag<V> tt) {
//    import spire.std.int._
    return toFrame(0);
  }

  Option<K> closestKeyTo(
      K k, double tolerance, MetricSpace<K, double> K0, Order<K> K1) {
//    apply(k) match {
//      case Value(v) => Some(k)
//      case _ =>
//        keys.collectFirst {
//          case key if MetricSpace.closeTo[K, Double](k, key, tolerance) => key
//        }
//    }
  }

  /// Map the keys of this series. This will maintain the same iteration
  /// order as the old series.
  Series<L, V> mapKeys(L f(K a)) {
//    return new Series(index.map((k, i) => f(k) -> i), column);
  }

  /// Map the values of this series only. Note that the function [f] will be
  /// called every time a value is accessed. To prevent this, you must
  /// `compact` the [Series].
  Series<K, W> mapValues(W f(V a)) => new Series(index, column.map(f));

  /// Map the values of this series, using both the *key* and *value* of each
  /// cell.
  Series<K, W> mapValuesWithKeys(W f(Tuple2<K, V> a)) {
    Builder<W> bldr = Column.newBuilder();
    index.foreach((k, row) {
      bldr += column(row).map((v) => f(k, v));
    });
    return Series(index.resetIndices, bldr.result());
  }

  /// Map the value of this series to a cell. This allows values to be turned
  /// into [NonValue]s (ie. [NA] and [NM]).
  Series<K, W> flatMapCell(Cell<W> f(V a)) =>
      new Series(index, column.flatMap(f));

  /// Transforms the cells in this series using [f].
  Series<K, W> cellMap(Cell<W> f(Cell<V> a)) {
    Builder<W> bldr = Column.newBuilder();
    index.foreach((k, row) {
      bldr += f(column(row));
    });
    return new Series(index.resetIndices, bldr.result());
  }

  /// Transforms the cells, indexed by their key, in this series using [f].
  Series<K, W> cellMapWithKeys(Cell<W> f(Tuple2<K, Cell<V>> a)) {
    Builder<W> bldr = Column.newBuilder();
    index.foreach((k, row) {
      bldr += f(k, column(row));
    });
    return new Series(index.resetIndices, bldr.result());
  }

  /// Select all key-cell pairs of this series where the pairs
  /// satisfy a predicate.
  ///
  /// This method preserves the orderedness of the underlying index.
  Series<K, V> filterEntries(bool p(Tuple2<K, Cell<V>> a)) {
    Builder<K, V> b = Series.newBuilder(index.isOrdered);
    b.sizeHint(index.size);
    for (var tup in index) {
      var k = tup.v1, ix = tup.v2;
      var cell = column(ix);
      if (p(k, cell)) {
        b.append(k, cell);
      }
    }
    return b.result();
  }

  /// Select all key-cell pairs of this series where the keys
  /// satisfy a predicate.
  ///
  /// This method preserves the orderedness of the underlying index.
  ///
  /// This method is a specialized and optimized version of
  /// [filterEntries], where
  ///
  ///     s.filterEntries { (k, _) => p(k) } == s.filterByKeys(p)
  Series<K, V> filterByKeys(bool p(K a)) {
    var b = new Series<K, V>.newBuilder(index.isOrdered);
    b.sizeHint(this.size);
    for (var i = 0; i < index.size; i += 1) {
      val k = index.keyAt(i);
      if (p(k)) {
        b.append(k, column(index.indexAt(i)));
      }
    }
    return b.result();
  }

  /// Select all key-cell pairs of this series where the cells
  /// satisfy a predicate.
  ///
  /// This method preserves the orderedness of the underlying index.
  ///
  /// This method is a specialized and optimized version of
  /// [filterEntries], where
  ///
  ///     s.filterEntries { (_, c) => p(c) } == s.filterByCells(p)
  Series<K, V> filterByCells(bool p(Cell<V> a)) {
    Builder<K, V> b = Series.newBuilder(index.isOrdered);
    b.sizeHint(this.size);
    for (var i = 0; i < index.size; i += 1) {
      val cell = column(index.indexAt(i));
      if (p(cell)) {
        b.append(index.keyAt(i), cell);
      }
    }
    return b.result();
  }

  /// Selects all key-value pairs of this series where the values
  /// satisfy a predicate.
  ///
  /// This method preserves the orderedness of the underlying index.
  /// It also assumes this series is dense, so any non values will
  /// also be filtered out. The column that backs the new series
  /// will be dense.
  ///
  /// This method is a specialized and optimized version of
  /// [filterEntries], where
  ///
  ///     s.filterEntries {
  ///       case (_, Value(v)) => p(v)
  ///       case _ => false
  ///     } == s.filterByValues(p)
  Series<K, V> filterByValues(bool p(V a)) {
    var b = new Series<K, V>.newBuilder(index.isOrdered);
    b.sizeHint(this.size);
    column.foreach(0, index.size, index.indexAt(_), false, (i, v) {
      if (p(v)) {
        b.appendValue(index.keyAt(i), v);
      }
    });
    return b.result();
  }

  /// Returns the first defined result of [f] when scanning the series in
  /// acending order.
  ///
  /// The parameter [f] is predicate on the key-values pairs of the
  /// series; however, it also returns a value when satisfied, hence
  /// the return type of `Option<B>`.
  ///
  /// To ensure efficient access to the values of the series, the
  /// predicate is supplied with the column and the index into the
  /// column, rather than the cell. (Contrast `(K, Cell<V>)` to
  /// `(K, Column<V>, int)`).
  ///
  /// [findFirstValue] is defined as,
  ///
  ///     findAsc((key, col, row) =>
  ///       if (col.isValueAt(row))
  ///         Some(key -> column.valueAt(row))
  ///       else
  ///         None
  ///     )
  Option<B> findAsc(Option<B> f(Tuple3<K, Column<V>, int> a)) {
    var i = 0;
    while (i < index.size) {
      var row = index.indexAt(i);
      var res = f(index.keyAt(i), column, row);
      if (res.isDefined) {
        return res;
      }
      i += 1;
    }
    return None;
  }

  /// Returns the first key-value in the series.
  ///
  /// The returned key-value pair is the first in the series where
  /// the value is both available and meaningful.
  Option<Tuple2<K, V>> findFirstValue() {
    findAsc((key, col, row) {
//      col.foldRow(row, None, None, value => Some(key -> value));
    });
  }

  /// Returns the first defined result of [f] when scanning the series in
  /// descending order.
  ///
  /// The parameter [f] is predicate on the key-values pairs of the
  /// series; however, it also returns a value when satisfied, hence
  /// the return type of `Option<B>`.
  ///
  /// To ensure efficient access to the values of the series, the
  /// predicate is supplied with the column and the index into the
  /// column, rather than the cell. (Contrast `(K, Cell<V>)` to
  /// `(K, Column<V>, int)`).
  ///
  /// [findLastValue] is defined as,
  ///
  ///     findDesc((key, col, row) {
  ///       if (col.isValueAt(row))
  ///         return Some(key -> col.valueAt(row));
  ///       else
  ///         return None;
  ///     });
  Option<B> findDesc(Option<B> f(Tuple3<K, Column<V>, int> a)) {
    var i = index.size - 1;
    while (i >= 0) {
      val row = index.indexAt(i);
      val res = f(index.keyAt(i), column, row);
      if (res.isDefined) {
        return res;
      }
      i -= 1;
    }
    return None;
  }

  /// Returns the last key-value in the series.
  ///
  /// The returned key-value pair is the last in the series where
  /// the value is both available and meaningful.
  Option<Tuple2<K, V>> findLastValue() {
    findDesc((key, col, row) {
//      col.foldRow(row)(None, None, value => Some(key -> value))
    });
  }

  /// Returns a compacted version of this [Series]. The new series will
  /// be equal to the old one, but the backing column will be dropped and
  /// replaced with a version that only contains the values needed for this
  /// series. It will also remove any indirection in the underlying column,
  /// such as that caused by reindexing, shifting, mapping values, etc.
  Series<K, V> compacted() =>
      new Series(index.resetIndices, column.reindex(index.indices));

  /// Reduce all the values in this [Series] using the given reducer.
  Cell<W> reduce(Reducer<V, W> reducer) {
    var indices = new List<int>(index.size);
    for (var i = 0; i < indices.length; i += 1) {
      indices[i] = index.indexAt(i);
    }
    return reducer.reduce(column, indices, 0, index.size);
  }

  /// Returns the [reduce.Count] reduction of this series.
  Cell<int> count() => this.reduce(framian.reduce.Count);

  /// Returns the [reduce.First] reduction of this series.
  Cell<V> first() => this.reduce(framian.reduce.First /*<V>*/);

  /// Returns the [reduce.FirstN] reduction of this series.
  Cell<List<V>> firstN(int n) => this.reduce(framian.reduce.FirstN(n));

  /// Returns the [reduce.Last] reduction of this series.
  Cell<V> last() => this.reduce(framian.reduce.Last /*<V>*/);

  /// Returns the [reduce.LastN] reduction of this series.
  Cell<List<V>> lastN(int n) => this.reduce(framian.reduce.LastN(n));

  /// Returns the [reduce.Max] reduction of this series.
  Cell<V> max(Order<V> ev0) => this.reduce(framian.reduce.Max /*<V>*/);

  /// Returns the [reduce.Min] reduction of this series.
  Cell<V> min(Order<V> ev0) => this.reduce(framian.reduce.Min /*<V>*/);

  /// Returns the `AdditiveMonoid` reduction of this series.
  Cell<V> sum(AdditiveMonoid<V> ev0) =>
      this.reduce(framian.reduce.MonoidReducer(ev0.additive));

  /// Returns the `AdditiveSemigroup` reduction of this series.
  Cell<V> sumNonEmpty(AdditiveSemigroup<V> ev0) =>
      this.reduce(framian.reduce.SemigroupReducer(ev0.additive));

  /// Returns the `MultiplicativeMonoid` reduction of this series.
  Cell<V> product(MultiplicativeMonoid<V> ev0) =>
      this.reduce(framian.reduce.MonoidReducer(ev0.multiplicative));

  /// Returns the `MultiplicativeSemigroup` reduction of this series.
  Cell<V> productNonEmpty(MultiplicativeSemigroup<V> ev0) =>
      this.reduce(framian.reduce.SemigroupReducer(ev0.multiplicative));

  /// Returns the [reduce.Mean] reduction of this series.
  Cell<V> mean(Field<V> ev0) => this.reduce(framian.reduce.Mean);

  /// Returns the [reduce.Median] reduction of this series.
  Cell<V> median(ClassTag<V> ev0, Field<V> ev1, Order<V> ev2) =>
      this.reduce(framian.reduce.Median);

  /// Returns the [reduce.Unique] reduction of this series.
  Cell<Set<V>> unique() => this.reduce(framian.reduce.Unique);

  /// Returns the [reduce.Exists] reduction of this series.
  bool exists(bool p(V a)) {
    var cell = this.reduce(framian.reduce.Exists(p));
    assume(
        cell.isValue, "assumed that the Exists reducer always returns a value");
    return cell.get();
  }

  /// Returns the [reduce.ForAll] reduction of this series.
  bool forAll(bool p(V a)) {
    var cell = this.reduce(framian.reduce.ForAll(p));
    assume(
        cell.isValue, "assumed that the ForAll reducer always returns a value");
    return cell.get();
  }

  /// For each unique key in this series, this reduces all the values for
  /// that key and returns a series with only the unique keys and reduced
  /// values. The new series will be in key order.
  Series<K, W> reduceByKey(Reducer<V, W> reducer) {
    var reduction = new Reduction<K, V, W>(column, reducer);
    var g = Index.group(index, reduction).result();
    var keys = g.v1, values = g.v2;
    return new Series(new Index.ordered(keys), new Column(values /*: _**/));
  }

  /// Returns the [reduce.Count] reduction of this series by key.
  Series<K, int> countByKey() => this.reduceByKey(framian.reduce.Count);

  /// Returns the [reduce.First] reduction of this series by key.
  Series<K, V> firstByKey() => this.reduceByKey(framian.reduce.First);

  /// Returns the [reduce.FirstN] reduction of this series by key.
  Series<K, List<V>> firstNByKey(int n) =>
      this.reduceByKey(framian.reduce.FirstN(n));

  /// Returns the [reduce.Last] reduction of this series by key.
  Series<K, V> lastByKey() => this.reduceByKey(framian.reduce.Last);

  /// Returns the [reduce.LastN] reduction of this series by key.
  Series<K, List<V>> lastNByKey(int n) =>
      this.reduceByKey(framian.reduce.LastN(n));

  /// Returns the [reduce.Max] reduction of this series by key.
  Series<K, V> maxByKey(Order<V> ev0) => this.reduceByKey(framian.reduce.Max);

  /// Returns the [reduce.Min] reduction of this series by key.
  Series<K, V> minByKey(Order<V> ev0) => this.reduceByKey(framian.reduce.Min);

  /// Returns the `AdditiveMonoid` reduction of this series by key.
  Series<K, V> sumByKey(AdditiveMonoid<V> ev0) =>
      this.reduceByKey(framian.reduce.MonoidReducer(ev0.additive));

  /// Returns the `AdditiveSemigroup` reduction of this series by key.
  Series<K, V> sumNonEmptyByKey(AdditiveSemigroup<V> ev0) =>
      this.reduceByKey(framian.reduce.SemigroupReducer(ev0.additive));

  /// Returns the `MultiplicativeMonoid` reduction of this series by key.
  Series<K, V> productByKey(MultiplicativeMonoid<V> ev0) =>
      this.reduceByKey(framian.reduce.MonoidReducer(ev0.multiplicative));

  /// Returns the `MultiplicativeSemigroup` reduction of this series by key.
  Series<K, V> productNonEmptyByKey(MultiplicativeSemigroup<V> ev0) =>
      this.reduceByKey(framian.reduce.SemigroupReducer(ev0.multiplicative));

  /// Returns the [reduce.Mean] reduction of this series by key.
  Series<K, V> meanByKey(Field<V> ev0) => this.reduceByKey(framian.reduce.Mean);

  /// Returns the [reduce.Median] reduction of this series by key.
  Series<K, V> medianByKey(ClassTag<V> ev0, Field<V> ev1, Order<V> ev2) =>
      this.reduceByKey(framian.reduce.Median);

  /// Returns the [reduce.Unique] reduction of this series by key.
  Series<K, Set<V>> uniqueByKey() => this.reduceByKey(framian.reduce.Unique);

  /// Returns the [reduce.Exists] reduction of this series by key.
  Series<K, bool> existsByKey(bool p(V a)) =>
      this.reduceByKey(framian.reduce.Exists(p));

  /// Returns the [reduce.ForAll] reduction of this series by key.
  Series<K, bool> forallByKey(bool p(V a)) =>
      this.reduceByKey(framian.reduce.ForAll(p));

  /// Rolls values and `NM`s forward, over `NA`s. This is similar to
  /// [rollForwardUpTo], but has no bounds checks. In fact, this is exactly
  /// equivalent to
  /// `series.rollForwardUpTo(1)(TrivialMetricSpace<K>, Order[Int])`.
  Series<K, V> rollForward() {
//    return rollForwardUpTo[Int](1, TrivialMetricSpace<K>, Order[Int]);
  }

  /// Roll-forward values and `NM`s over `NA`s. It will rolls values in sequence
  /// order (not sorted order). It will only roll over `NA`s whose key is within
  /// `delta` of the last valid value or `NM`. This bounds check is inclusive.
  ///
  /// An example of this behaviour is as follows:
  /// ```
  /// Series(1 -> "a", 2 -> NA, 3 -> NA, 4 -> NM, 5 -> NA, 6 -> NA).rollForwardUpTo(1D) ===
  ///     Series(1 -> "a", 2 -> "a", 3 -> NA, 4 -> NM, 5 -> NM, 6 -> NA)
  /// ```
  Series<K, V> rollForwardUpTo(R delta) {
    List<int> indices = new List<int>(index.size);

    loop(int i, int lastValue) {
      if (i < index.size) {
        var row = index.indexAt(i);
        if (column(row) == NA) {
          if (K.distance(index.keyAt(i), index.keyAt(lastValue)) <= delta) {
            indices[i] = index.indexAt(lastValue);
          } else {
            indices[i] = row;
          }
          return loop(i + 1, lastValue);
        } else {
          indices[i] = row;
          return loop(i + 1, i);
        }
      }
    }

    return loop(0, 0);

    return new Series(index, column.reindex(indices));
  }

  /// Compute a histogram of the values in this series, using a bucket width of
  /// `stepSize`. This just calls `histogram(min, max, stepSize)`, where `min`
  /// and `max` are the minimum and maximum values in this series.
  Series<Tuple2<V, V>, int> histogram(
      V stepSize, AdditiveSemigroup<V> g, Order<V> o, ClassTag<V> ct) {
//    val hist = for {
//      minValue <- min
//      maxValue <- max
//    } yield histogram(minValue, maxValue, stepSize)

    return hist.getOrElse(new Series<Tuple2<V, V>, int>.empty());
  }

  /// Computes a histogram of the values in this series. This will create
  /// contiguous, disjoint buckets starting from [min], each with width
  /// [stepSize], except for (possibly) the last. So, we could represent the
  /// buckets like this,
  ///
  /// ```
  /// [min, min + stepSize), [min + 2 * stepSize), ..., [min + k * stepSize, max]
  /// ```
  ///
  /// You'll note that the last bucket *includes* [max]. For each bucket, we
  /// calculate the number of values from this series that fall within it. The
  /// [Series] returned is keyed by a tuple of the start and end of each
  /// bucket (keep in mind only the last bucket is inclusive on the right).
  Series<Tuple2<V, V>, int> histogram2(V min, V max, V stepSize,
      AdditiveSemigroup<V> g, Order<V> o, ClassTag<V> ct) {
    var bucketStarts = Stream
        .iterate(min, (i) => i + stepSize)
        .takeWhile((v) => v < max)
        .toArray();
    val index = Index.ordered(bucketStarts);
    List count = Array.fill(index.size, 0);
    foreachValues((v) {
      // The insert index isn't quite what we want. If v exists in the index,
      // then we can use it directly. If it doesn't, then `-insertIndex - 1`
      // actually points at the *next* bucket, since that is where it would be
      // inserted to maintain sorted order. So, we subtract 1 more (hence
      // `-insertIndex - 2`) so it points at the right bucket. But this isn't
      // enough! We also need to make sure we don't include values > max in
      // the histogram and that max is inclusive in the last bucket.
      var insertIndex = index.search(v);
      var i = (insertIndex < 0) ? -insertIndex - 2 : insertIndex;
      if (i == (count.length - 1) && v <= max) {
        count[count.length - 1] += 1;
      } else if (i >= 0 && i < (count.length - 1)) {
        count[i] += 1;
      }
    });
    var buckets = bucketStarts.map((start) {
//      start -> spire.math.min(max, start + stepSize);
    });
    return new Series(Index.ordered(buckets), Column.dense(count));
  }

  /// Compute a histogram of the values in this series, using a bucket width of
  /// [stepSize]. This just calls `histogram(min, max, stepSize)`, where `min`
  /// and `max` are the minimum and maximum values in this series. The values
  /// are the proportion of values that fell into that bucket, as a percentage
  /// of the total size of this series - including NAs and NMs.
  Series<Tuple2<V, V>, A> normalizedHistogram(
      V stepSize, AdditiveSemigroup<V> g, Order<V> o, ClassTag<V> ct) {
//    val hist = for {
//      min <- min
//      max <- max
//    } yield normalizedHistogram[A](min, max, stepSize)
    return hist.getOrElse(new Series<Tuple2<V, V>, A>.empty());
  }

  /// Computes a normalized histogram of the values in this series. This will
  /// create contiguous, disjoint buckets starting from `min`, each with width
  /// `stepSize`, except for (possibly) the last. So, we could represent the
  /// buckets like this,
  ///
  /// ```
  /// [min, min + stepSize), [min + 2 * stepSize), ..., [min + k * stepSize, max]
  /// ```
  ///
  /// You'll note that the last bucket *includes* `max`. For each bucket, we
  /// calculate the number of values from this series that fall within it. The
  /// [Series] returned is keyed by a tuple of the start and end of each
  /// bucket (keep in mind only the last bucket is inclusive on the right). The
  /// values are the proportion of values that fell into that bucket, as a
  /// percentage of the total size of this series - including NAs and NMs.
  Series<Tuple2<V, V>, A> normalizedHistogram2(V min, V max, V stepSize,
      AdditiveSemigroup<V> g, Order<V> o, ClassTag<V> ct) {
    var n = new Field<A>.fromInt(index.size);
    return histogram(min, max, stepSize).mapValues((cnt) {
      return new Field<A>.fromInt(cnt) / n;
    });
  }

  @override
  String toString() {
    zip([keys, cells]).map((z) {
      var key = z[0], cell = z[1];
      return "$key -> $cell";
    }).mkString("Series(", ", ", ")");
  }

  @override
  bool operator ==(that0) {
//    that0 match {
//      case (that: Series[_, _]) if this.index.size == that.index.size =>
//        (this.index.iterator zip that.index.iterator) forall {
//          case ((key0, idx0), (key1, idx1)) if key0 == key1 =>
//            this.column(idx0) == that.column(idx1)
//          case _ => false
//        }
//      case _ => false
//    }
  }

  @override
  int get hashCode {
    return index.map((k, i) => new Tuple2(k, column(i))).hashCode;
  }

  static Series<K, V> empty() =>
      new Series(new Index<K>.empty(), new Column<V>.empty());

//  factory Series(Index<K> index, Column<V> column) => new Series(index, column)

  factory Series.fromPairs(Iterable<Tuple<K, V>> kvs) {
    var un = kvs.unzip();
    var keys = un.v1, values = un.v2;
    return new Series(new Index(keys.toArray()), new Column.values(values));
  }

  static Series<int, V> fromValues(Iterable<V> values) {
    var keys = range(values.length - 1);
    return new Series(new Index(keys), new Column.values(values));
  }

  static Series<K, V> fromCells(Iterable<Tuple2<K, Cell<V>>> col) {
    var bldr = Series.newUnorderedBuilder();
//    bldr ++= col;
    return bldr.result();
  }

//  static Series<K, V> fromCells(Iterable<Tuple2<K, Cell<V>>> kvs) =>
//    fromCells(kvs);

  static Series<K, V> fromMap(Map<K, V> kvMap) => new Series(kvMap.toSeq());

//  implicit def cbf[K: Order: ClassTag, V]: CanBuildFrom[Series[_, _], (K, Cell<V>), Series<K, V>] =
//    new CanBuildFrom[Series[_, _], (K, Cell<V>), Series<K, V>] {
//      def apply(): mutable.Builder[(K, Cell<V>), Series<K, V>] = Series.newUnorderedBuilder[K ,V]
//      def apply(from: Series[_, _]): mutable.Builder[(K, Cell<V>), Series<K, V>] = apply()
//    }

  AbstractSeriesBuilder<K, V> newBuilder_(bool isOrdered) {
    return isOrdered ? newOrderedBuilder : newUnorderedBuilder;
  }

  AbstractSeriesBuilder<K, V> newUnorderedBuilder_() {
//    new AbstractSeriesBuilder<K, V> {
//      def result(): Series<K, V> = {
//        val index = Index(this.keyBldr.result())
//        val column = this.colBldr.result()
//        Series(index, column)
//      }
//    }
  }

  AbstractSeriesBuilder<K, V> newOrderedBuilder_() {
//    new AbstractSeriesBuilder<K, V> {
//      def result(): Series<K, V> =
//        Series(
//          Index.ordered(this.keyBldr.result()),
//          this.colBldr.result())
//    }
  }
}

abstract class _AbstractSeriesBuilder {
  //extends mutable.Builder<Tuple2<K, Cell<V>>, Series<K, V>> {
  var keyBldr = new Array<K>.newBuilder();
  var colBldr = new Column<V>.newBuilder();

  void operator +(Tuple2<K, Cell<V>> elem) {
    keyBldr += elem.v1;
    colBldr += elem.v2;
  }

  append(K k, Cell<V> c) {
    keyBldr += k;
    colBldr += c;
    return this;
  }

  appendValue(K k, V v) {
    keyBldr += k;
    colBldr.addValue(v);
    return this;
  }

  appendNonValue(K k, NonValue nonValue) {
    keyBldr += k;
    colBldr += nonValue;
    return this;
  }

  Unit clear() {
    keyBldr.clear();
    colBldr.clear();
  }

  @override
  Unit sizeHint(int size) {
    keyBldr.sizeHint(size);
    colBldr.sizeHint(size);
  }
}
