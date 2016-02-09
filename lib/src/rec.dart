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

class TypeWitness<A> {
  TypeWitness(A value /*, implicit val classTag: ClassTag<A>*/);

  static /*implicit*/ lift(A a) => new TypeWitness(a);
}

/// A `Rec` is an untyped sequence of values - usually corresponding
/// to a row or column in a Frame.
class Rec<K> {
  Rec(Series<K, UntypedColumn> cols, int row);

  Cell<A> get(K col) => cols(col).flatMap((c) => c.cast[A].apply(row));

  Iterable<Tuple<K, Cell>> values() {
//    cols.to[Vector].map((k, colCell) {
//    val value = for {
//      col <- colCell
//      a <- col.cast[Any].apply(row)
//    } yield a

    return new Tuple2(k, value);
  }

  @override
  String toString() {
    values.map((k, value) {
      return '$k -> ${value.fold("na", "nm", (v) => v.toString())}';
    }).mkString("Rec(", ", ", ")");
  }

  @override
  bool operator ==(that) {
    if (that is Rec) {
      return this.values == that.values;
    } else {
      return false;
    }
  }

  @override
  int get hashCode => this.values.hashCode * 23;

  factory Rec.fromPairs(Tuple2<K, TypeWitness> kvs) {
    Series<K, UntypedColumn> cols =
        new Series(kvs.map((k, TypeWitness /*[a]*/ w) {
      return new Tuple2(k, TypedColumn[a](Column.value(w.value))(w.classTag));
    }));
    return new Rec(cols, 0);
  }

  factory Rec.fromRow(Frame frame, int row) =>
      new Rec(frame.columnsAsSeries, row);

  factory Rec.fromCol(Frame frame, int col) => new Rec(frame.rowsAsSeries, col);

  /*implicit*/ RowExtractor<Rec<K>, K, Variable> RecRowExtractor =
      new _RecRowExtractor<Rec<K>, K, Variable>();
}

class _RecRowExtractor extends RowExtractor<Rec<K>, K, Variable> {
//  type P = Series[K, UntypedColumn]

  Option<P> prepare(Series<K, UntypedColumn> cols, List<K> keys) {
    return new Some(
        Series.fromCells(keys.map(((k) => new Tuple2(k, cols(k))))));
  }

  Cell<Rec<K>> extract(int row, P cols) => new Value(new Rec(cols, row));
}
