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

abstract class DenseColumn<A extends num> {
  //extends UnboxedColumn<A> {
  List values();
  Mask naValues();
  Mask nmValues();

  _valid(int row) => row >= 0 && row < values.length;
  bool isValueAt(int row) => valid(row) && !naValues(row) && !nmValues(row);
  NonValue nonValueAt(int row) => nmValues(row) ? NM : NA;

  Column<A> filter(bool p(A a)) {
    var na = Mask.newBuilder();
    var i = 0;
    while (i < values.length) {
      if (naValues(i) || (isValueAt(i) && !p(valueAt(i)))) {
        na += i;
      }
      i += 1;
    }
    return Column.dense(values, na.result(), nmValues).asInstanceOf[Column];
  }

  Column<A> mask(Mask na) => Column
      .dense(values, naValues | na, nmValues.dec(na))
      .asInstanceOf[Column];

  Column<A> setNA(int row) {
    if ((row < 0 || row >= values.length) && !nmValues(row)) {
      return this;
    } else {
      return Column.dense(values, naValues + row, nmValues - row).asInstanceOf[
          Column];
    }
  }

  Column<A> memoize(bool optimistic) => this;

  Column flatMap(Cell<B> f(A a)) {
    var bldr = Column.newBuilder();
    var i = 0;
    while (i < values.length) {
      if (nmValues(i)) {
        bldr.addNM();
      } else if (naValues(i)) {
        bldr.addNA();
      } else {
        bldr.add(f(valueAt(i)));
      }
      i += 1;
    }
    return bldr.result();
  }

  Column<A> shift(int n) {
    if (n < 0) {
      return Column.eval((a) => apply(a)).shift(n);
    } else {
      var len =
          spire.math.min(values.length.toLong + n, Int.MaxValue.toLong).toInt();
      var indices = Array.fill(len, -1);
      var i = n;
      while (i < len) {
        indices[i] = i - n;
        i += 1;
      }
      return reindex(indices);
    }
  }

  Column zipMap(Column that, dynamic f(A a, B b)) {
//    that match {
//    case (that: DenseColumn[_]) => DenseColumn.zipMap[A, B, C](this, that.asInstanceOf[DenseColumn[B]], f)
//    case _ => DenseColumn.zipMap[A, B, C](this, that.force(this.values.length).asInstanceOf[DenseColumn[B]], f)
  }

  @override
  String toString() {
    var len = nmValues.max.map((v) => v + 1).getOrElse(values.length);
    return "Column(" +
        range(len).map((i) => apply(i).toString()).join(", ") +
        ")";
  }
}

//private[framian] object DenseColumn extends DenseColumnFunctions

class IntColumn extends DenseColumn<int> {
  List<int> values;
  Mask naValues;
  Mask nmValues;

  IntColumn(this.values, this.naValues, this.nmValues);

  int valueAt(int row) => values(row);

  Column map(dynamic f(int a)) =>
      DenseColumn.mapInt(values, naValues, nmValues, f);

  Column<int> reindex(List<int> index) =>
      DenseColumn.reindexInt(index, values, naValues, nmValues);

  Column<int> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max.getOrElse(-1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return new IntColumn(values, naValues, nm);
    } else {
      return new IntColumn(
          Arrays.copyOf(values, len),
          (values.length < len)
              ? concat([naValues, Mask.range(values.length, len)])
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseInt(values, naValues, nmValues, that);
}

//private[framian] case class LongColumn(values: Array[Long], naValues: Mask, nmValues: Mask) extends DenseColumn[Long] {
//  def valueAt(row: Int): Long = values(row)
//  def map[@sp(Int,Long,Double) B](f: Long => B): Column[B] = DenseColumn.mapLong(values, naValues, nmValues, f)
//  def reindex(index: Array<int>): Column[Long] = DenseColumn.reindexLong(index, values, naValues, nmValues)
//  def force(len: Int): Column[Long] = {
//    if (values.length <= len) {
//      val nm = if (nmValues.max.getOrElse(-1) < len) nmValues
//               else nmValues.filter(_ < len)
//      LongColumn(values, naValues, nm)
//    } else {
//      LongColumn(
//        java.util.Arrays.copyOf(values, len),
//        if (values.length < len) naValues ++ Mask.range(values.length, len) else naValues,
//        nmValues.filter(_ < len)
//      )
//    }
//  }
//  def orElse[A0 >: Long](that: Column[A0]): Column[A0] = DenseColumn.orElseLong(values, naValues, nmValues, that)
//}

class DoubleColumn extends DenseColumn<double> {
  List<double> values;
  Mask naValues;
  Mask nmValues;

  DoubleColumn(List<double> values, Mask naValues, Mask nmValues);

  double valueAt(int row) => values(row);

  Column map(dynamic f(double a)) =>
      DenseColumn.mapDouble(values, naValues, nmValues, f);

  Column<double> reindex(List<int> index) =>
      DenseColumn.reindexDouble(index, values, naValues, nmValues);

  Column<double> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max.getOrElse(-1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return new DoubleColumn(values, naValues, nm);
    } else {
      return new DoubleColumn(
          Arrays.copyOf(values, len),
          (values.length < len)
              ? concat([naValues, Mask.range(values.length, len)])
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseDouble(values, naValues, nmValues, that);
}

class AnyColumn<A> extends DenseColumn<A> {
  List values;
  Mask naValues;
  Mask nmValues;

  AnyColumn(this.values, this.naValues, this.nmValues);

  A valueAt(int row) => values(row).asInstanceOf;

  Column map(dynamic f(A a)) =>
      DenseColumn.mapAny(values, naValues, nmValues, f);

  Column<A> reindex(List<int> index) =>
      DenseColumn.reindexAny(index, values, naValues, nmValues);

  Column<A> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max.getOrElse(-1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return AnyColumn(values, naValues, nm);
    } else {
      return AnyColumn(
          DenseColumn.copyArray(values, len),
          (values.length < len)
              ? concat([naValues, Mask.range(values.length, len)])
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseAny(values, naValues, nmValues, that);
}

class GenericColumn<A> extends DenseColumn<A> {
  List<A> values;
  Mask naValues;
  Mask nmValues;
  GenericColumn(List<A> values, Mask naValues, Mask nmValues);

  A valueAt(int row) => values(row);

  Column map(dynamic f(A a)) =>
      DenseColumn.mapGeneric(values, naValues, nmValues, f);

  Column<A> reindex(List<int> index) =>
      DenseColumn.reindexGeneric(index, values, naValues, nmValues);

  Column<A> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max.getOrElse(-1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return GenericColumn(values, naValues, nm);
    } else {
      return new GenericColumn(
          DenseColumn.copyArray(values, len),
          (values.length < len)
              ? concat([naValues, Mask.range(values.length, len)])
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseGeneric(values, naValues, nmValues, that);
}
