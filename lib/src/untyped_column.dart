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

/// An abstraction for heterogeneously typed columns. We work with them by
/// casting to a real, typed column. Values that cannot be cast are treated as
/// `NM` (not meaningful) values.
abstract class UntypedColumn extends ColumnLike<UntypedColumn> {
  Column<A> cast /*[A: ColumnTyper]*/ ();

  UntypedColumn orElse(UntypedColumn that) {
//      (this, that) match {
//      case (EmptyUntypedColumn, _) => that
//      case (_, EmptyUntypedColumn) => this
//      case _ => MergedUntypedColumn(this, that)
//    }
  }

  /*implicit object monoid extends Monoid[UntypedColumn] {
    def id: UntypedColumn = empty
    def op(lhs: UntypedColumn, rhs: UntypedColumn): UntypedColumn =
      lhs orElse rhs
  }*/

  static final UntypedColumn empty = EmptyUntypedColumn;
}

class EmptyUntypedColumn extends UntypedColumn {
  Column<A> cast /*[A: ColumnTyper]*/ () => new Column.empty<A>();
  UntypedColumn mask(Mask na) => EmptyUntypedColumn;
  UntypedColumn shift(Int rows) => EmptyUntypedColumn;
  UntypedColumn reindex(List<int> index) => EmptyUntypedColumn;
  UntypedColumn setNA(int row) => EmptyUntypedColumn;
}

class TypedColumn<A> extends UntypedColumn {
  TypedColumn(Column<A> column /*, implicit val classTagA: ClassTag<A>*/);

  Column<B> cast(/*implicit*/ ColumnTyper<B> typer) => typer.cast(this);
  UntypedColumn mask(Mask na) => TypedColumn(column.mask(na));
  UntypedColumn shift(int rows) => TypedColumn(column.shift(rows));
  UntypedColumn reindex(List<int> index) => TypedColumn(column.reindex(index));
  UntypedColumn setNA(int row) => TypedColumn(column.setNA(row));
}

class MergedUntypedColumn extends UntypedColumn {
  MergedUntypedColumn(UntypedColumn left, UntypedColumn right);

  Column<A> cast /*[A: ColumnTyper]*/ () =>
      left.cast /*<A>*/ ().orElse(right.cast /*<A>*/ ());
  UntypedColumn mask(Mask na) =>
      MergedUntypedColumn(left.mask(na), right.mask(na));

  UntypedColumn shift(int rows) =>
      MergedUntypedColumn(left.shift(rows), right.shift(rows));

  UntypedColumn reindex(List<int> index) =>
      MergedUntypedColumn(left.reindex(index), right.reindex(index));

  UntypedColumn setNA(int row) =>
      MergedUntypedColumn(left.setNA(row), right.setNA(row));
}

class ConcatColumn extends UntypedColumn {
  ConcatColumn(UntypedColumn col0, UntypedColumn col1, int offset);

  Column<A> cast /*[A: ColumnTyper]*/ () => col0.cast /*<A>*/ .force(offset)
      .orElse(col1.cast /*<A>*/ .shift(offset).mask(Mask.range(0, offset)));

  UntypedColumn mask(Mask na) => ConcatColumn(col0.mask(na),
      col1.mask(na.filter((v) => v >= offset).map((v) => v - offset)), offset);

  UntypedColumn shift(int rows) =>
      ConcatColumn(col0.shift(rows), col1.shift(rows), offset + rows);

  UntypedColumn reindex(List<int> index) {
    var index0 = index.map((row) {
      return (row < offset) ? row : offset;
    });
    var index1 = index.map((row) {
      return (row < offset) ? -1 : row - offset;
    });
    return MergedUntypedColumn(
        col0.setNA(offset).reindex(index0), col1.setNA(-1).reindex(index1));
  }

  UntypedColumn setNA(int row) {
    if (row < offset) {
      return ConcatColumn(col0.setNA(row), col1, offset);
    } else {
      return ConcatColumn(col0, col1.setNA(row - offset), offset);
    }
  }
}
