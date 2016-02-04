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

abstract class Frame<Row, Col> {
  /** The row keys' index. */
  Index<Row> rowIndex;

  /** The column keys' index. */
  Index<Col> colIndex;

  Frame() {
    rowClassTag = rowIndex.classTag;
    rowOrder = rowIndex.order;
    colClassTag = colIndex.classTag;
    colOrder = colIndex.order;
  }

  var rowClassTag;
  var rowOrder;
  var colClassTag;
  var colOrder;

  /** A column-oriented view of this frame. Largely used internally. */
  Series<Col, UntypedColumn> columnsAsSeries;

  /** A row-oriented view of this frame. Largely used internally. */
  Series<Row, UntypedColumn> rowsAsSeries;

  /**
   * Returns `true` if this frame can be treated as column oriented. This is
   * largely for optimization purposes.
   */
  bool isColOriented;

  /**
   * Returns `true` if this frame can be treated as row oriented. This is
   * largely for optimization purposes.
   */
  bool isRowOriented;

  /**
   * Transposes the rows and columns of this frame. All rows in this frame
   * becomes the columns of the new frame.
   */
  Frame<Col, Row> transpose();

  /**
   * Returns the set of all unique column keys.
   */
  Set<Col> colKeys() => colIndex.map((c) => c.v1, collection.breakOut);

  /**
   * Returns the set of all unique row keys.
   */
  Set<Row> rowKeys() => rowIndex.map((r) => r.v1, collection.breakOut);

  /**
   * Returns the number of rows in this frame.
   */
  int rows() => rowIndex.size;

  /**
   * Returns the number of cols in this frame.
   */
  int cols() => colIndex.size;

  /**
   * Returns `true` if this frame is logically empty. A frame is logically
   * empty if none of its rows or columns contain a value, though they may
   * contain [[NA]]s or [[NM]]s.
   *
   * TODO: I think an NM should also count as a "value".
   */
  bool isEmpty() {
    return columnsAsSeries.iterator.collectFirst(() {
//      case (id, Value(column)) if Series(rowIndex, column.cast[Any]).hasValues => id
    }).isEmpty();
  }

  /** The following methods allow a user to apply reducers directly across a frame. In
    * particular, this API demands that we specify the type that the reducer accepts and
    * it will only apply it in the case that there exists a type conversion for a given
    * column.
    */
  Series<Col, R> reduceFrame(Reducer<V, R> reducer) {
    return columnsAsSeries.flatMapCell((col) {
      return new Series(rowIndex, col.cast[V]).reduce(reducer);
    });
  }

  Frame<Row, Col> reduceFrameByKey(Reducer<V, R> reducer) {
//    columnsAsSeries.cellMap {
//      case NA => Value(Series(rowIndex, Column.empty[V]()))
//      case Value(col) => Value(Series(rowIndex, col.cast[V]))
//      case NM => NM
//    }.denseIterator.foldLeft(Frame.empty<Row, Col>) { case (acc, (key, series)) =>
//      acc.join(key, series.reduceByKey(reducer))(Join.Outer)
//    }
  }

  Series<Col, C> reduceFrameWithCol(Col col, Reducer<Tuple2<A, B>, C> reducer) {
    var fixed = column[A](col);
//    Series.fromCells(columnsAsSeries.to[List].collect {
//      case (k, Value(untyped)) if k != col =>
//        val series = Series(rowIndex, untyped.cast[B])
//        k -> (fixed zip series).reduce(reducer)
//    }: _*)
  }

  Frame<Row, Col> getColumnGroup(Col col) => withColIndex(colIndex.getAll(col));

  Frame<Row, Col> getRowGroup(Row row) => withRowIndex(rowIndex.getAll(row));

  Frame<R1, C1> mapRowGroups(Frame<R1, C1> f(Row a, Frame<Row, Col> b)) {
//    object grouper extends Index.Grouper<Row> {
//      case class State(rows: Int, keys: Vector[Array[R1]], cols: Series[C1, UntypedColumn]) {
//        def result(): Frame[R1, C1] = ColOrientedFrame(Index(Array.concat(keys: _*)), cols)
//      }
//
//      def init = State(0, Vector.empty, Series.empty)
//      def group(state: State)(keys: Array<Row>, indices: Array[Int], start: Int, end: Int): State = {
//        val groupRowIndex = Index(keys.slice(start, end), indices.slice(start, end))
//        val groupKey = keys(start)
//        val group = ColOrientedFrame(groupRowIndex, columnsAsSeries)
//
//        val State(offset, groupKeys, cols) = state
//        val result = f(groupKey, group)
//        val newCols = cols.merge(result.columnsAsSeries mapValues {
//          _.reindex(result.rowIndex.indices).shift(offset)
//        })
//        State(offset + result.rowIndex.size, groupKeys :+ result.rowIndex.keys, newCols)
//      }
//    }

    return new Index.group(rowIndex, grouper).result();
  }

  // Row/Column Index manipulation.

  /** Replaces the column index with `index`. */
  Frame<Row, C1> withColIndex(Index<C1> index);

  /** Replaces the row index with `index`. */
  Frame<R1, Col> withRowIndex(Index<R1> index);

  /**
   * Put the columns in sorted order. This affects only the traversal order of
   * the columns.
   */
  Frame<Row, Col> sortColumns() => withColIndex(colIndex.sorted);

  /**
   * Put the rows in sorted order. This affects only the traversal order of the
   * rows.
   */
  Frame<Row, Col> sortRows() => withRowIndex(rowIndex.sorted);

  /**
   * Reverse the traversal order of the columns in this frame.
   */
  Frame<Row, Col> reverseColumns() => withColIndex(colIndex.reverse);

  /**
   * Reverse the traversal order of the rows in this frame.
   */
  Frame<Row, Col> reverseRows() => withRowIndex(rowIndex.reverse);

  /**
   * Removes rows whose row key is true for the predicate `p`.
   */
  filterRowKeys(bool p(Row a)) =>
      withRowIndex(rowIndex.filter((row, _) => p(row)));

  /**
   * Removes rows whose row key is true for the predicate `p`.
   */
  filterColKeys(bool p(Col c)) =>
      withColIndex(colIndex.filter((col, _) => p(col)));

  /**
   * Map the row index using `f`. This retains the traversal order of the rows.
   */
  Frame<R, Col> mapRowKeys(R f(Row r)) =>
      this.withRowIndex(rowIndex.map((k, v) => new Tuple2(f(k), v)));

  /**
   * Map the column index using `f`. This retains the traversal order of the
   * columns.
   */
  Frame<Row, C> mapColKeys(C f(Col c)) =>
      this.withColIndex(colIndex.map((k, v) => new Tuple2(f(k), v)));

  /**
   * Retain only the cols in `cols`, dropping all others.
   */
  Frame<Row, Col> retainColumns(Iterable<Col> cols) =>
      filterColKeys(cols.toSet());

  /**
   * Retain only the rows in `rows`, dropping all others.
   */
  Frame<Row, Col> retainRows(Iterable<Row> rows) => filterRowKeys(rows.toSet());

  /**
   * Drop the columns `cols` from the column index. This simply removes the
   * columns from the column index and does not modify the actual columns.
   */
  Frame<Row, Col> dropColumns(Iterable<Col> cols) =>
      filterColKeys((k) => !cols.contains(k));

  /**
   * Drop the rows `rows` from the row index. This simply removes the rows
   * from the index and does not modify the actual columns.
   */
  Frame<Row, Col> dropRows(Iterable<Row> rows) =>
      filterRowKeys((k) => !rows.contains(k));

  /**
   * Returns the value of the cell at row `rowKey` and column `colKey` as the
   * type `A`. If the value doesn't exist, then [[NA]] is returned. If the
   * value is not meaningful as an instance of `A`, then [[NM]] is returned.
   */
  Cell<A> apply(Row rowKey, Col colKey) {
//    for {
//      col <- columnsAsSeries(colKey)
//      row <- Cell.fromOption(rowIndex get rowKey)
//      a <- col.cast[A].apply(row)
//    } yield a
  }

  /** Returns a single column from this `Frame` cast to type `T`. */
  Series<Row, T> column(Col col) => get(Cols(col).as[T]);

  /** Returns a single row from this `Frame` cast to type `T`. */
  Series<Col, T> row(Row row) => get(Rows(row).as[T]);

  Option<Rec<Col>> getRow(Row key) => rowIndex.get(key).map(Rec.fromRow(this));

  Option<Rec<Row>> getCol(Col key) => colIndex.get(key).map(Rec.fromCol(this));

  // Cols/Rows based ops.

  /**
   * Extract values from the columns of the series using `rows` and returns
   * them in a [[Series]].
   */
  Series<Col, A> get(Rows<Row, A> rows) =>
      Frame.extract(colIndex, rowsAsSeries, rows);

  /**
   * Extract values from the rows of the series using `cols` and returns them
   * in a [[Series]].
   */
  Series<Row, A> get(Cols<Col, A> cols) =>
      Frame.extract(rowIndex, columnsAsSeries, cols);

  /**
   * Re-indexes the frame using the extractor `cols` to define the new row
   * keys. This will not re-order the frame rows, just replace the keys. This
   * will drop any rows where `cols` extracts a [[NonValue]].
   *
   * To retain `NA`s or `NM`s in the index, you'll need to recover your
   * [[Cols]] with some other value. For example,
   *
   * {{{
   * frame.reindex(cols.map(Value(_)).recover { case nonValue => nonValue })
   * }}}
   */
  Frame<A, Col> reindex(Cols<Col, A> cols) =>
      withRowIndex(Frame.reindex(rowIndex, columnsAsSeries, cols));

  /**
   * Re-indexes the frame using the extractor `rows` to define the new row
   * keys. This will not re-order the frame cols, just replace the keys. This
   * will drop any columns where `rows` extracts a [[NonValue]].
   *
   * To retain `NA`s or `NM`s in the index, you'll need to recover your
   * [[Rows]] with some other value. For example,
   *
   * {{{
   * frame.reindex(rows.map(Value(_)).recover { case nonValue => nonValue })
   * }}}
   */
  Frame<Row, A> reindex(Rows<Row, A> rows) =>
      withColIndex(Frame.reindex(colIndex, rowsAsSeries, rows));

  /**
   * Maps each row to a value using `rows`, then maps the result with the
   * column key using `f` and stores it in the row `to`. If `to` doesn't exist
   * then a new row will be appended onto the frame, otherwise the row(s) with
   * key `to` will be removed first.
   */
  Frame<Row, Col> mapWithIndex(
          Rows<Row, A> rows, Row to, f(Tuple2<Col, A> a)) =>
      transpose.mapWithIndex(rows.toCols, to)(f).transpose();

  /**
   * Maps each column to a value using `cols`, then maps the result with the row
   * key using `f` and stores it in the column `to`. If `to` doesn't exist
   * then a new column will be appended onto the frame, otherwise the
   * columns(s) with key `to` will be removed first.
   */
  Frame<Row, Col> mapWithIndex(
          Cols<Col, A> cols, Col to, f(Tuple2<Row, A> a)) =>
      dropColumns(to).merge(
          to,
          Frame.extract(rowIndex, columnsAsSeries, cols).mapValuesWithKeys(f),
          Merge.Outer);

  /**
   * Extracts a row from this frame using [[Rows]], then merges it back into
   * this frame as the row `to`. If `to` doesn't exist then a new row will be
   * appended onto the frame, otherwise the row(s) with key `to` will be
   * removed first.
   */
  Frame<Row, Col> map(Rows<Row, A> rows, Row to, B f(A a)) =>
      transpose().map(rows.toCols(), to, f).transpose();

  /**
   * Extracts a column from this frame using [[Cols]], then merges it back into
   * this frame as the column `to`. If `to` doesn't exist then a new column
   * will be appended onto the frame, otherwise the columns(s) with key `to`
   * will be removed first.
   *
   * This is equivalent to, but may be more efficient than
   * `frame.merge(to, frame.get(cols))(Merge.Outer)`.
   */
  Frame<Row, Col> map(Cols<Col, A> cols, Col to, B f(A a)) =>
      dropColumns(to).merge(to,
          Frame.extract(rowIndex, columnsAsSeries, cols.map(f)), Merge.Outer);

  /**
   * Filter this frame using `cols` extract values and the predicate `p`. If,
   * for a given row, `p` is false, then that row will be removed from the
   * frame. Any [[NA]] and [[NM]] rows will also be removed.
   */
  Frame<Row, Col> filter(Cols<Col, A> cols, bool p(A a)) =>
      withRowIndex(Frame.filter(rowIndex, columnsAsSeries, cols.map(p)));

  /**
   * Filter this frame using `rows` extract values and the predicate `p`. If,
   * for a given column, `p` is false, then that column will be removed from the
   * frame. Any [[NA]] and [[NM]] columns will also be removed.
   */
  Frame<Row, Col> filter(Rows<Row, A> rows, bool p(A a)) =>
      withColIndex(Frame.filter(colIndex, rowsAsSeries, rows.map(p)));

  /**
   * Sorts the frame using the order for the [[Rows]] provided. This will only
   * ever permute the cols of the frame and will not remove/add anything.
   *
   * @param rows The column value extractor to get the sort key
   */
  Frame<Row, Col> sortBy(Rows<Row, A> rows) =>
      withColIndex(Frame.sortBy(colIndex, rowsAsSeries, rows));

  /**
   * Sorts the frame using the order for the [[Cols]] provided. This will only
   * ever permute the rows of the frame and will not remove/add anything.
   *
   * @param cols The row value extractor to get the sort key
   */
  Frame<Row, Col> sortBy(Cols<Col, A> cols) =>
      withRowIndex(Frame.sortBy(rowIndex, columnsAsSeries, cols));

  /**
   * This "groups" the frame rows using the [[Cols]] extractor to determine the
   * group for each row. Each row is then re-keyed using its group.
   *
   * This is equivalent to, but more efficient than, `frame.sortBy(cols).reindex(cols)`.
   */
  Frame<A, Col> group(Cols<Col, A> cols) =>
      withRowIndex(Frame.group(rowIndex, columnsAsSeries, cols));

  /**
   * This "groups" the frame cols using the [[Rows]] extractor to determine the
   * group for each column. Each column is then re-keyed using its group.
   *
   * This is equivalent to, but more efficient than, `frame.sortBy(rows).reindex(rows)`.
   */
  Frame<Row, A> group(Rows<Row, A> rows) =>
      withColIndex(Frame.group(colIndex, rowsAsSeries, rows));

  /**
   * Reduces this frame using `cols` and joins the result back into the frame.
   * This doesn't remove any rows from the frame; the reduced result is
   * duplicated for each row. This will replace the `to` column if it exists,
   * otherwise it creates a new column `to` at the end.
   *
   * {{{
   * scala&gt; val f = Frame.fromRows("a" :: 2 :: HNil, "b" :: 3 :: HNil)
   * scala&gt; f.reduce(Cols(1).as[Int], 1)(reduce.Sum)
   * res0: Frame[Int, Int] =
   *     0 . 1
   * 0 : a | 5
   * 1 : b | 5
   * }}}
   */
  Frame<Row, Col> reduce(Cols<Col, A> cols, Col to, Reducer<A, B> reducer) {
    var cell = get(cols).reduce(reducer);
    var result = Series(rowIndex, Column.eval((_) => cell));
    return dropColumns(to).merge(to, result)(Merge.Outer);
  }

  /**
   * Reduces this frame using `rows` and joins the result back into the frame.
   * This doesn't remove any columns from the frame; the reduced result is
   * duplicated for each column. This will replace the `to` row if it exists,
   * otherwise it creates a new row `to` at the end.
   *
   * {{{
   * scala&gt; val f = Frame.fromColumns("a" :: 2 :: HNil, "b" :: 3 :: HNil)
   * scala&gt; f.reduce(Cols(1).as[Int], 1)(reduce.Sum)
   * res0: framian.Frame[Int,Int] =
   *     0 . 1
   * 0 : a | b
   * 1 : 5 | 5
   * }}}
   */
  Frame<Row, Col> reduce(Rows<Row, A> rows, Row to, Reducer<A, B> reducer) =>
      transpose().reduce(rows.toCols, to, reducer).transpose();

  /**
   * Reduces this frame, by row key groups, using `cols` and joins the result
   * back into the frame. Within each row key group, the reduced result is
   * duplicated for each row. If `to` exists it will be replaced, otherwise it
   * will be added to the end of the columns.
   */
  Frame<Row, Col> reduceByKey(
          Cols<Col, A> cols, Col to, Reducer<A, B> reducer) =>
      dropColumns(to).join(to, get(cols).reduceByKey(reducer), Join.Outer);

  /**
   * Reduces this frame, by column key groups, using `rows` and joins the
   * result back into the frame. Within each column key group, the reduced
   * result is duplicated for each column. If `to` exists it will be replaced,
   * otherwise it will be added to the end of the rows.
   */
  Frame<Row, Col> reduceByKey(
          Rows<Row, A> rows, Row to, Reducer<A, B> reducer) =>
      transpose().reduceByKey(rows.toCols(), to, reducer).transpose();

  /**
   * Appends the rows in `that` to the end of the rows in `this`. This will
   * force the columns into sorted order.
   */
  Frame<Row, Col> appendRows(Frame<Row, Col> that) {
    if (this.isColOriented) {
      var un1 = this.rowIndex.unzip();
      var keys0 = un1.v1, indices0 = un1.v2;
      var un2 = that.rowIndex.unzip();
      var keys1 = un2.v1, indices1 = un2.v2;
      var cols0 = this.columnsAsSeries.mapValues((s) => s.reindex(indices0));
      var cols1 = that.columnsAsSeries.mapValues((s) => s.reindex(indices1));
      var offset = keys0.size;
//      var cols = cols0.combine(cols1, (col) => col, _.shift(offset), (col0, col1) {
//        ConcatColumn(col0, col1, offset)
//      });
      return new ColOrientedFrame(new Index(concat([keys0, keys1])), cols);
    } else {
      var merger = new Merger<Col>(Merge.Outer);
      var cg = Index.cogroup(this.colIndex, that.colIndex)(merger).result();
      var keys = cg.v1, lIndices = cg.v2, rIndices = cg.v3;
      var rows0 = this.rowsAsSeries.mapValues((s) => s.reindex(lIndices));
      var rows1 = that.rowsAsSeries.mapValues((s) => s.reindex(rIndices));
      return new RowOrientedFrame(new Index(keys), concat([rows0, rows1]));
    }
  }

  /**
   * Appends the columns in `that` to the end of the columns in `this`. This
   * will force the rows into sorted order.
   */
  Frame<Row, Col> appendCols(Frame<Row, Col> that) =>
      transpose().appendRows(that.transpose()).transpose();

  @override
  int get hashCode {
    var values = columnsAsSeries.iterator.flatMap((colKey, cell) {
      var col = cell.getOrElse(UntypedColumn.empty).cast[Any];
      return new Series(rowIndex, col).iterator.map((rowKey, value) {
        return new Tuple3(rowKey, colKey, value);
      });
    });
    return values.toList().hashCode;
  }

  @override
  bool operator ==(that) {
    if (that is Frame) {
      var cols0 = this.columnsAsSeries;
      var cols1 = that.columnsAsSeries;
      var rowIndex0 = this.rowIndex;
      var rowIndex1 = that.rowIndex;
      var keys0 = rowIndex0.map((r) => r.v1);
      var keys1 = rowIndex1.map((r) => r.v1);
//      (cols0.size == cols1.size) && (keys0 == keys1) && (cols0.iterator zip cols1.iterator).forall {
//        case ((k0, v0), (k1, v1)) if k0 == k1 =>
//          def col0 = v0.getOrElse(UntypedColumn.empty).cast[Any]
//          def col1 = v1.getOrElse(UntypedColumn.empty).cast[Any]
//          (v0 == v1) || (Series(rowIndex0, col0) == Series(rowIndex1, col1))
//
//        case _ => false
//      }
    } else {
      return false;
    }
  }

  @override
  String toString() {
    String pad(String repr, int width) => repr + (" " * (width - repr.length));

    List<String> justify(List<String> reprs) {
      var width = reprs.maxBy((s) => s.length).length;
      return reprs.map((r) => pad(r, width));
    }

    List<String> collapse(List<String> keys, List<List<String>> cols) {
      List<String> loop(
          List<String> keys0, List<List<String>> cols0, List<String> lines) {
//        keys0 match {
//          case key :: keys1 =>
//            val row = cols0 map (_.head)
//            val line = key + row.mkString(" : ", " | ", "")
//            loop(keys1, cols0 map (_.tail), line :: lines)
//
//          case Nil =>
//            lines.reverse
//        }
      }

      var header =
          keys.head() + cols.map((c) => c.head()).mkString("   ", " . ", "");
//      header :: loop(keys.tail(), cols.map((c) => c.tail()), Nil);
    }

//    var keys = justify("" :: rowIndex.map((r) => r.v1.toString()).toList());
    var cols = columnsAsSeries.iterator.map((key, cell) {
      var header = key.toString();
      var col = cell.getOrElse(UntypedColumn.empty).cast[Any];
//      val values: List<String> = Series(rowIndex, col).iterator.map {
//        case (_, Value(value)) => value.toString
//        case (_, nonValue) => nonValue.toString
//      }.toList
//      justify(header :: values)
    }).toList();

    return collapse(keys, cols).mkString("\n");
  }

  Frame<Row, Col> genericJoin_(
      Frame<Row, Col> that, Index.GenericJoin<Row> cogrouper) {
    var state = Index.cogroup(this.rowIndex, that.rowIndex, cogrouper);
    var res = state.result();
    var keys = res.v1, lIndex = res.v2, rIndex = res.v3;
    var lCols = this.columnsAsSeries.mapValues(_.setNA(Skip).reindex(lIndex));
    var rCols = that.columnsAsSeries.mapValues(_.setNA(Skip).reindex(rIndex));
    return new ColOrientedFrame(Index.ordered(keys), concat([lCols, rCols]));
  }

  Frame<Row, Col> merge(Frame<Row, Col> that, Merge mergeStrategy) =>
      genericJoin(that, new Merger(mergeStrategy));

  Frame<Row, Col> merge(Col col, Series<Row, T> that, Merge mergeStrategy) =>
      merge(that.toFrame(col), mergeStrategy);

  Frame<Row, Col> merge(
          L them, Merge merge, Frame.SeriesMergeFolder<L, Row, Col> folder) =>
      them.foldLeft(this, Frame.mergeSeries);

  Frame<Row, Col> join(Frame<Row, Col> that, Join joinStrategy) =>
      genericJoin(that)(Joiner(joinStrategy));

  Frame<Row, Col> join(Col col, Series<Row, T> that, Join joinStrategy) =>
      join(that.toFrame(col), joinStrategy);

  Frame<Row, Col> join(
          L them, Join join, Frame.SeriesJoinFolder<L, Row, Col> folder) =>
      them.foldLeft(this, Frame.joinSeries);

  /**
   * Create an empty `Frame` with no values.
   */
  static Frame<Row, Col> empty() => new ColOrientedFrame<Row, Col>(
      new Index.empty<Row>(), new Index.empty<Col>(), Column.empty());

  /**
   * Populates a homogeneous `Frame` given the rows/columns of the table. The
   * value of each cell is calculated using `f`, applied to its row and column
   * index.
   *
   * For instance, we can make a multiplication table,
   *
   * {{{
   * Frame.fill(1 to 9, 1 to 9) { (row, col) => Value(row * col) }
   * }}}
   */
  static Frame<A, B> fill(
      Iterable<A> rows, Iterable<B> cols, Cell<C> f(Tuple2<A, B> a)) {
    var rows0 = rows.toVector;
    var cols0 = cols.toVector;
    var columns = Column.dense(cols0.map((b) {
//      return TypedColumn(Column(rows0.map((a) => f(a, b)))): UntypedColumn
    }).toArray());
    return new ColOrientedFrame(
        Index.fromKeys(rows0), Index.fromKeys(cols0), columns);
  }

  /**
   * Construct a Frame whose rows are populated from some type `A`. Row
   * populators may exist for things like JSON objects or Shapeless Generic
   * types. For example,
   *
   * {{{
   * scala&gt; case class Person(name: String, age: Int)
   * scala&gt; val Alice = Person("Alice", 42)
   * scala&gt; val Bob = Person("Alice", 23)
   * scala&gt; Frame.fromRows(Alice, Bob)
   * res0: Frame[Int, Int] =
   *     0     . 1
   * 0 : Alice | 42
   * 1 : Bob   | 23
   * }}}
   *
   * TODO: This should really take the row too (eg. `rows: (Row, A)*`).
   */
  Frame<int, Col> fromRows(Iterable<A> rows, RowPopulator<A, int, Col> pop) {
//    return pop.frame(rows.zipWithIndex.foldLeft(pop.init, (state, (data, row)) {
//      return pop.populate(state, row, data);
//    }));
  }

  /**
   * Construct a Frame whose columns are populated from some type `A`. Column
   * populators may exist for things like JSON objects or Shapeless Generic
   * types. For example,
   *
   * {{{
   * scala&gt; case class Person(name: String, age: Int)
   * scala&gt; val Alice = Person("Alice", 42)
   * scala&gt; val Bob = Person("Alice", 23)
   * scala&gt; Frame.fromRows(Alice, Bob)
   * res0: Frame[Int, Int] =
   *     0     . 1
   * 0 : Alice | Bob
   * 1 : 42    | 23
   * }}}
   */
  Frame<Row, int> fromColumns(
          Iterable<A> cols, RowPopulator<A, int, Row> pop) =>
      fromRows(cols).transpose();

  // Here by dragons, devoid of form...

  /** A polymorphic function for joining many [[Series]] into a `Frame`. */
//  object joinSeries extends Poly2 {
//    implicit def colSeriesPair[A: ClassTag: ColumnTyper, Row, Col] =
//      at[Frame<Row, Col>, (Col, Series<Row, A>)] { case (frame, (col, series)) =>
//        frame.join(col, series)(Join.Outer)
//      }
//  }

  /** A polymorphic function for merging many [[Series]] into a `Frame`. */
//  object mergeSeries extends Poly2 {
//    implicit def colSeriesPair[A: ClassTag: ColumnTyper, Row, Col] =
//      at[Frame<Row, Col>, (Col, Series<Row, A>)] { case (frame, (col, series)) =>
//        frame.merge(col, series)(Merge.Outer)
//      }
//  }

  /** A left fold on an HList that creates a Frame from a set of [[Series]]. */
//  type SeriesJoinFolder[L <: HList, Row, Col] = LeftFolder.Aux[L, Frame<Row, Col>, joinSeries.type, Frame<Row, Col>]

  /** A left fold on an HList that creates a Frame from a set of [[Series]]. */
//  type SeriesMergeFolder[L <: HList, Row, Col] = LeftFolder.Aux[L, Frame<Row, Col>, mergeSeries.type, Frame<Row, Col>]

  /** Implicit to help with inference of Row/Col in mergeColumn/mergeRows. Please ignore... */
//  trait KeySeriesPair[L <: HList, Row, Col]
//  object KeySeriesPair {
//    import shapeless.::
//    implicit def hnilKeySeriesPair<Row, Col> = new KeySeriesPair[HNil, Row, Col] {}
//    implicit def hconsKeySeriesPair[A, T <: HList, Row, Col](implicit
//        t: KeySeriesPair[T, Row, Col]) = new KeySeriesPair[(Col, Series<Row, A>) :: T, Row, Col] {}
//  }

  /**
   * Given an HList of `(Col, Series[Row, V])` pairs, this will build a
   * `Frame<Row, Col>`, outer-merging all of the series together as the columns
   * of the frame.
   *
   * The use of `Generic.Aux` allows us to use auto-tupling (urgh) to allow
   * things like `Frame.mergeColumns("a" -> seriesA, "b" -> seriesB)`, rather
   * than having to use explicit `HList`s.
   *
   * @usecase def mergeColumn<Row, Col>(cols: (Col, Series[Row, _])*): Frame<Row, Col>
   */
//  Frame<Row, Col> mergeColumns[S, L <: HList, Col, Row](cols: S)(implicit
//      gen: Generic.Aux[S, L],
//      ev: KeySeriesPair[L, Row, Col],
//      folder: SeriesMergeFolder[L, Row, Col],
//      ctCol: ClassTag<Col>, orderCol: Order<Col>,
//      ctRow: ClassTag<Row>, orderRow: Order<Row>) =>
//    gen.to(cols).foldLeft(new Frame.empty<Row, Col>, mergeSeries)

  /**
   * Given an HList of `(Row, Series[Col, V])` pairs, this will build a
   * `Frame<Row, Col>`, outer-merging all of the series together as the rows
   * of the frame.
   *
   * The use of `Generic.Aux` allows us to use auto-tupling (urgh) to allow
   * things like `Frame.mergeRows("a" -> seriesA, "b" -> seriesB)`, rather
   * than having to use explicit `HList`s.
   *
   * @usecase def mergeRows<Row, Col>(rows: (Row, Series[Col, _])*): Frame<Row, Col>
   */
//  def mergeRows[S, L <: HList, Col, Row](rows: S)(implicit
//      gen: Generic.Aux[S, L],
//      ev: KeySeriesPair[L, Col, Row],
//      folder: SeriesMergeFolder[L, Col, Row],
//      ctCol: ClassTag<Col>, orderCol: Order<Col>,
//      ctRow: ClassTag<Row>, orderRow: Order<Row>): Frame<Row, Col> =
//    gen.to(rows).foldLeft(Frame.empty[Col, Row])(mergeSeries).transpose

  // Axis-agnostic frame operations.

  Index<A> reindex_(
      Index<I> index, Series<K, UntypedColumn> cols, AxisSelection<K, A> sel) {
    var bldr = Index.newBuilder();
//    sel.foreach(index, cols) { (_, row, cell) =>
//      cell.foreach(bldr.add(_, row))
//    }
    bldr.result();
  }

  Index<I> sortBy_(
      Index<I> index, Series<K, UntypedColumn> cols, AxisSelection<K, A> sel) {
    ArrayBuffer<Tuple3<Cell<A>, I, int>> buffer = new ArrayBuffer();
//    sel.foreach(index, cols) { (key, row, sortKey) =>
//      buffer += ((sortKey, key, row))
//    }
    var bldr = Index.newBuilder();
    var pairs = buffer.toArray();
    pairs.qsortBy((p) => p.v1);
    for (var i = 0; i < pairs.length; i += 1) {
      var p = pairs(i);
      var key = p.v2, idx = p.v3;
      bldr.add(key, idx);
    }
    return bldr.result();
  }

  Index<A> group_(
      Index<I> index, Series<K, UntypedColumn> cols, AxisSelection<K, A> sel) {
    var bldr = Index.newBuilder();
//    sel.foreach(index, cols) { (_, row, cell) =>
//      cell.foreach(bldr.add(_, row));
//    }
    return bldr.result().sorted();
  }

  Series<I, A> extract_(
      Index<I> index, Series<K, UntypedColumn> cols, AxisSelection<K, A> sel) {
    val bldr = Column.newBuilder();
//    sel.foreach(index, cols) { (_, _, cell) =>
//      bldr += cell
//    }
    return Series(index.resetIndices, bldr.result());
  }

  Index<I> filter_(Index<I> index, Series<K, UntypedColumn> cols,
      AxisSelection<K, bool> sel) {
    var bldr = Index.newBuilder();
//    sel.foreach(index, cols) {
//      case (key, row, Value(true)) => bldr.add(key, row)
//      case _ =>
//    }
    return bldr.result();
  }
}

class ColOrientedFrame<Row, Col> extends Frame<Row, Col> {
  Index<Row> rowIndex;
  Index<Col> colIndex;
  Column<UntypedColumn> valueCols;

  ColOrientedFrame(this.rowIndex, this.colIndex, this.valueCols);

  Series<Col, UntypedColumn> columnsAsSeries() =>
      new Series(colIndex, valueCols);
  Series<Row, UntypedColumn> rowsAsSeries() => new Series(
      rowIndex,
      Column.eval((row) {
        return Value(RowView(colIndex, valueCols, row));
      }).memoize());

  Frame<Row, C1> withColIndex(Index<C1> ci) =>
      new ColOrientedFrame(rowIndex, ci, valueCols);

  Frame<R1, Col> withRowIndex(Index<R1> ri) =>
      new ColOrientedFrame(ri, colIndex, valueCols);

  Frame<Col, Row> transpose() =>
      new RowOrientedFrame(colIndex, rowIndex, valueCols);

  bool isColOriented = true;
  bool isRowOriented = false;

  factory ColOrientedFrame(
          Index<Row> rowIdx, Series<Col, UntypedColumn> cols) =>
      ColOrientedFrame(rowIdx, cols.index, cols.column);
}

class RowOrientedFrame<Row, Col> extends Frame<Row, Col> {
  Index<Row> rowIndex;
  Index<Col> colIndex;
  Column<UntypedColumn> valueRows;

  RowOrientedFrame(this.rowIndex, this.colIndex, this.valueRows);

  Series<Row, UntypedColumn> rowsAsSeries() => new Series(rowIndex, valueRows);
  Series<Col, UntypedColumn> columnsAsSeries() => new Series(
      colIndex,
      Column.eval((row) {
        return new Value(new RowView(colIndex, valueRows, row));
      }).memoize());

  Frame<Row, C1> withColIndex(Index<C1> ci) =>
      new RowOrientedFrame(rowIndex, ci, valueRows);

  Frame<R1, Col> withRowIndex(Index<R1> ri) =>
      new RowOrientedFrame(ri, colIndex, valueRows);

  Frame<Col, Row> transpose() =>
      new ColOrientedFrame(colIndex, rowIndex, valueRows);

  bool isColOriented = false;
  bool isRowOriented = true;

  factory RowOrientedFrame(
          Index<Col> colIdx, Series<Row, UntypedColumn> rows) =>
      new RowOrientedFrame(rows.index, colIdx, rows.column);
}

class RowView<K> extends UntypedColumn {
  Index<K> index;
  Column<UntypedColumn> cols;
  int row;
  var trans;

  RowView(this.index, this.cols, this.row
//        UntypedColumn trans(UntypedColumn c) = RowView.DefaultTransform
      );

  Column<B> cast() {
    Column.eval((colIdx) {
//      for {
//        col <- if (colIdx >= 0 && colIdx < index.size) cols(index.indexAt(colIdx))
//               else NA
//        value <- trans(col).cast[B].apply(row)
//      } yield value
    });
  }

  transform_(UntypedColumn f(UntypedColumn c)) =>
      new RowView(index, cols, row, trans.andThen(f));

  UntypedColumn mask(Mask na) => transform((m) => m.mask(na));

  UntypedColumn shift(int rows) => transform((r) => r.shift(rows));

  UntypedColumn reindex(List<int> index) => transform((i) => i.reindex(index));

  UntypedColumn setNA(int na) => transform((t) => t.setNA(na));

  static var DefaultTransform = (UntypedColumn uc) => a;
}
