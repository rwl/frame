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

abstract class DenseColumn<A> extends UnboxedColumn<A> {
  DenseColumn();

  List get values;
  Mask get naValues;
  Mask get nmValues;

  _valid(int row) => row >= 0 && row < values.length;
  bool isValueAt(int row) => _valid(row) && !naValues[row] && !nmValues[row];
  NonValue nonValueAt(int row) => nmValues[row] ? NM : NA;

  Column<A> filter(bool p(A a)) {
    var na = new MaskBuilder();
    var i = 0;
    while (i < values.length) {
      if (naValues[i] || (isValueAt(i) && !p(valueAt(i)))) {
        na.add(i);
      }
      i += 1;
    }
    return new Column.dense(values, na.result(), nmValues);
  }

  Column<A> mask(Mask na) =>
      new Column.dense(values, naValues | na, nmValues.dec(na));

  Column<A> setNA(int row) {
    if ((row < 0 || row >= values.length) && !nmValues[row]) {
      return this;
    } else {
      return new Column.dense(values, naValues + row, nmValues - row);
    }
  }

  Column<A> memoize([bool optimistic = false]) => this;

  Column flatMap(Cell f(A a)) {
    var bldr = new ColumnBuilder();
    var i = 0;
    while (i < values.length) {
      if (nmValues[i]) {
        bldr.addNM();
      } else if (naValues[i]) {
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
      return new Column.eval((a) => apply(a)).shift(n);
    } else {
      int len = math.min(values.length + n, MAX_INT);
      var indices = new List<int>.generate(len, (_) => -1);
      var i = n;
      while (i < len) {
        indices[i] = i - n;
        i += 1;
      }
      return reindex(indices);
    }
  }

  Column zipMap(Column that, dynamic f(A a, b)) {
    if (that is DenseColumn) {
      return _zipMap(this, that, f);
    } else {
      return _zipMap(this, that.force(this.values.length) as DenseColumn, f);
    }
  }

  Column _zipMap(DenseColumn<A> _lhs, DenseColumn _rhs, dynamic f(A a, b)) {
    var _len = math.min(_lhs.values.length, _rhs.values.length);
    var na = _lhs.naValues | _rhs.naValues;
    var nm = (_lhs.nmValues | _rhs.nmValues).filter((i) => i < _len && !na(i));

    List<A> lhs = _lhs.values;
    List rhs = _rhs.values;

    var len = math.min(lhs.length, rhs.length);

    Column loopAny(List xs, int i) {
      if (i < xs.length) {
        if (na(i) || nm(i)) {
          return loopAny(xs, i + 1);
        } else {
          xs[i] = f(lhs[i], rhs[i]);
          return loopAny(xs, i + 1);
        }
      } else {
        return AnyColumn(xs, na, nm);
      }
    }

    Column loopInt(Int32List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        if (!(na(i) || nm(i))) {
          try {
            xs[i] = f(lhs[i], rhs[i]) as int;
          } on CastError catch (_) {
            return loopAny(_copyToAnyArray(xs, i), i);
          }
        }
        i += 1;
      }

      return new IntColumn(xs, na, nm);
    }

    Column loopDouble(Float64List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        if (!(na(i) || nm(i))) {
          try {
            xs[i] = f(lhs[i], rhs[i]) as double;
          } on CastError catch (_) {
            return loopAny(_copyToAnyArray(xs, i), i);
          }
        }
        i += 1;
      }

      return new DoubleColumn(xs, na, nm);
    }

    Column loop(int i) {
      if (i < lhs.length && i < rhs.length) {
        if (na(i) || nm(i)) {
          return loop(i + 1);
        } else {
          var x = f(lhs[i], rhs[i]);
          if (x is int) {
            var xs = new List<int>(len);
            xs[i] = x;
            return loopInt(xs, i + 1);
          } else if (x is num) {
            var xs = new List<double>(len);
            xs[i] = x;
            return loopDouble(xs, i + 1);
          } else {
            var xs = new List(len);
            xs[i] = x;
            return loopAny(xs, i + 1);
          }
        }
      } else {
        return new Column.empty(nm);
      }
    }

    return loop(0);
  }

  @override
  String toString() {
    var len = nmValues.max().map((v) => v + 1).getOrElse(() => values.length);
    return "Column(" +
        range(len).map((i) => apply(i).toString()).join(", ") +
        ")";
  }
}

class IntColumn extends DenseColumn<int> {
  Int32List values;
  Mask naValues;
  Mask nmValues;

  IntColumn(this.values, this.naValues, this.nmValues);

  int valueAt(int row) => values[row];

  Column map(dynamic f(int a)) =>
      DenseColumn.mapInt(values, naValues, nmValues, f);

  Column<int> reindex(List<int> index) =>
      DenseColumn.reindexInt(index, values, naValues, nmValues);

  static Int32List _copyOf(Int32List l, int size) {
    var r = new Int32List(size);
    for (var i = 0; i < math.min(l.length, size); i++) {
      r[i] = l[i];
    }
    return r;
  }

  Column<int> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max().getOrElse(() => -1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return new IntColumn(values, naValues, nm);
    } else {
      return new IntColumn(
          _copyOf(values, len),
          (values.length < len)
              ? naValues.inc(new Mask.range(values.length, len))
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseInt(values, naValues, nmValues, that);
}

class LongColumn extends DenseColumn<int> {
  Int64List values;
  Mask naValues;
  Mask nmValues;

  LongColumn(this.values, this.naValues, this.nmValues);

  int valueAt(int row) => values[row];

  Column map(dynamic f(int a)) =>
      DenseColumn.mapLong(values, naValues, nmValues, f);

  Column<int> reindex(List<int> index) =>
      DenseColumn.reindexLong(index, values, naValues, nmValues);

  static Int64List _copyOf(Int64List l, int size) {
    var r = new Int64List(size);
    for (var i = 0; i < math.min(l.length, size); i++) {
      r[i] = l[i];
    }
    return r;
  }

  Column<int> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max().getOrElse(() => -1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return new LongColumn(values, naValues, nm);
    } else {
      return new LongColumn(
          _copyOf(values, len),
          (values.length < len)
              ? naValues.inc(new Mask.range(values.length, len))
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseLong(values, naValues, nmValues, that);
}

class DoubleColumn extends DenseColumn<double> {
  Float64List values;
  Mask naValues;
  Mask nmValues;

  DoubleColumn(this.values, this.naValues, this.nmValues);

  double valueAt(int row) => values[row];

  Column map(dynamic f(double a)) =>
      DenseColumn.mapDouble(values, naValues, nmValues, f);

  Column<double> reindex(List<int> index) =>
      DenseColumn.reindexDouble(index, values, naValues, nmValues);

  static Float64List _copyOf(Float64List l, int size) {
    var r = new Float64List(size);
    for (var i = 0; i < math.min(l.length, size); i++) {
      r[i] = l[i];
    }
    return r;
  }

  Column<double> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max().getOrElse(() => -1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return new DoubleColumn(values, naValues, nm);
    } else {
      return new DoubleColumn(
          _copyOf(values, len),
          (values.length < len)
              ? naValues.inc(new Mask.range(values.length, len))
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column that) =>
      DenseColumn.orElseDouble(values, naValues, nmValues, that);
}

class GenericColumn<A> extends DenseColumn<A> {
  List<A> values;
  Mask naValues;
  Mask nmValues;

  GenericColumn(this.values, this.naValues, this.nmValues) : super();

  A valueAt(int row) => values[row];

  Column map(dynamic f(A a)) => _map(values, naValues, nmValues, f);

  Column _map(List<A> values, Mask naValues, Mask nmValues, dynamic f(A a)) {
    Column loopAny(List xs, int i) {
      if (i < xs.length) {
        if (naValues[i] || nmValues[i]) {
          return loopAny(xs, i + 1);
        } else {
          xs[i] = f(values[i]);
          return loopAny(xs, i + 1);
        }
      } else {
        return new AnyColumn(xs, naValues, nmValues);
      }
    }

    Column loopInt(Int32List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        if (!(naValues[i] || nmValues[i])) {
          try {
            xs[i] = f(values[i]) as int;
          } on CastError catch (_) {
            return loopAny(_copyToAnyArray(xs, i), i);
          }
        }
        i += 1;
      }
      return new IntColumn(xs, naValues, nmValues);
    }

    Column loopDouble(Float64List xs, int i0) {
      var i = i0;
      while (i < xs.length) {
        if (!(naValues[i] || nmValues[i])) {
          try {
            xs[i] = f(values[i]) as double;
          } on CastError catch (_) {
            return loopAny(_copyToAnyArray(xs, i), i);
          }
        }
        i += 1;
      }
      return new DoubleColumn(xs, naValues, nmValues);
    }

    Column loop(int i) {
      if (i < values.length) {
        if (naValues[i] || nmValues[i]) {
          return loop(i + 1);
        } else {
          var x = f(values[i]);
          if (x is int) {
            var xs = new Int32List(values.length);
            xs[i] = x;
            return loopInt(xs, i + 1);
          } else if (x is num) {
            var xs = new Float64List(values.length);
            xs[i] = x;
            return loopDouble(xs, i + 1);
          } else {
            var xs = new List(values.length);
            xs[i] = x;
            return loopAny(xs, i + 1);
          }
        }
      } else {
        return new Column.empty(nmValues);
      }
    }

    return loop(0);
  }

  Column<A> reindex(List<int> index) =>
      DenseColumn.reindexGeneric(index, values, naValues, nmValues);

  List<A> _copyOf(List<A> l, int size) {
    var r = new List<A>(size);
    for (var i = 0; i < math.min(l.length, size); i++) {
      r[i] = l[i];
    }
    return r;
  }

  Column<A> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max().getOrElse(() => -1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return new GenericColumn(values, naValues, nm);
    } else {
      return new GenericColumn(
          _copyOf(values, len),
          (values.length < len)
              ? naValues.inc(new Mask.range(values.length, len))
              : naValues,
          nmValues.filter((v) => v < len));
    }
  }

  Column orElse(Column rhs) {
    if (rhs is DenseColumn) {
      var bldr = new ColumnBuilder();
      var len = math.max(values.length, rhs.values.length);
      var i = 0;
      while (i < len) {
        if (i < values.length && !naValues[i] && !nmValues[i]) {
          bldr.addValue(values[i]);
        } else if (rhs.isValueAt(i)) {
          bldr.addValue(rhs.valueAt(i));
        } else if (nmValues[i]) {
          bldr.addNM();
        } else {
          bldr.add(rhs.nonValueAt(i));
        }
        i += 1;
      }
      return bldr.result();
    } else {
      // TODO: Add case for unboxed columns.
      return new Column.eval((row) {
        if (row >= 0 &&
            row < values.length &&
            !naValues[row] &&
            !nmValues[row]) {
          return new Value(values[row]);
        } else {
          var cell = rhs.apply(row);
          if (cell == NA && nmValues[row]) {
            return NM;
          } else {
            return cell;
          }
        }
      });
    }
  }
}

class AnyColumn extends GenericColumn {
  AnyColumn(List values, Mask naValues, Mask nmValues)
      : super(values, naValues, nmValues);
}

List _copyToAnyArray(List xs, int len) {
  var ys = new List(xs.length);
  var i = 0;
  while (i < xs.length && i < len) {
    ys[i] = xs[i];
    i += 1;
  }
  return ys;
}
