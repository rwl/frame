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

abstract class DenseColumn<A extends Comparable> extends UnboxedColumn<A> {
  List<A> get values;
  final Mask naValues;
  final Mask nmValues;

  DenseColumn(this.naValues, this.nmValues);

  _valid(int row) => row >= 0 && row < values.length;

  bool isValueAt(int row) => _valid(row) && !naValues[row] && !nmValues[row];

  NonValue nonValueAt(int row) => nmValues[row] ? NM : NA;

  A valueAt(int row) => values[row];

  List<A> _mkList(int size);
  Column<A> _mkCol(List<A> values, Mask naValues, Mask nmValues);
  ColumnBuilder<A> _mkColBldr();

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
    var bldr = new AnyColumnBuilder();
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
      return _zipMap(this, that.force(values.length) as DenseColumn, f);
    }
  }

  static Column _zipMap(DenseColumn _lhs, DenseColumn _rhs, dynamic f(a, b)) {
    var _len = math.min(_lhs.values.length, _rhs.values.length);
    var na = _lhs.naValues | _rhs.naValues;
    var nm = (_lhs.nmValues | _rhs.nmValues).filter((i) => i < _len && !na(i));

    List lhs = _lhs.values;
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
        return new AnyColumn(xs, na, nm);
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

  Column map(dynamic f(A a)) {
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

  Column<A> reindex(List<int> index) {
    var xs = _copyOf(values, index.length);
    var na = new MaskBuilder();
    var nm = new MaskBuilder();
    var i = 0;
    while (i < index.length) {
      var row = index[i];
      if (nmValues[row]) {
        nm.add(i);
      } else if (row >= 0 && row < values.length && !naValues[row]) {
        xs[i] = values[row];
      } else {
        na.add(i);
      }
      i += 1;
    }
    return _mkCol(xs, na.result(), nm.result());
  }

  List _copyOf(List l, int size) {
    List r = _mkList(size);
    for (var i = 0; i < l.length && i < size; i++) {
      r[i] = l[i];
    }
    return r;
  }

  Column orElse(Column rhs) {
    if (rhs is DenseColumn) {
      var bldr = _mkColBldr();
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

  Column<A> force(int len) {
    if (values.length <= len) {
      var nm = (nmValues.max().getOrElse(() => -1) < len)
          ? nmValues
          : nmValues.filter((v) => v < len);
      return _mkCol(values, naValues, nm);
    } else {
      return _mkCol(
          _copyOf(values, len),
          (values.length < len)
              ? naValues.inc(new Mask.range(values.length, len))
              : naValues,
          nmValues.filter((v) => v < len));
    }
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
  final Int32List values;

  IntColumn(this.values, Mask naValues, Mask nmValues)
      : super(naValues, nmValues);

  List<int> _mkList(int size) => new Int32List(size);

  Column<int> _mkCol(Int32List values, Mask naValues, Mask nmValues) =>
      new IntColumn(values, naValues, nmValues);

  ColumnBuilder<int> _mkColBldr() => new IntColumnBuilder();
}

class DoubleColumn extends DenseColumn<double> {
  final Float64List values;

  DoubleColumn(this.values, Mask naValues, Mask nmValues)
      : super(naValues, nmValues);

  List<double> _mkList(int size) => new Float64List(size);

  Column<double> _mkCol(Float64List values, Mask naValues, Mask nmValues) =>
      new DoubleColumn(values, naValues, nmValues);

  ColumnBuilder<double> _mkColBldr() => new DoubleColumnBuilder();
}

class GenericColumn<A extends Comparable> extends DenseColumn<A> {
  final List<A> values;

  GenericColumn(this.values, Mask naValues, Mask nmValues)
      : super(naValues, nmValues);

  List<A> _mkList(int size) => new List<A>(size);

  Column<A> _mkCol(List<A> values, Mask naValues, Mask nmValues) =>
      new GenericColumn<A>(values, naValues, nmValues);

  ColumnBuilder<A> _mkColBldr() => new GenericColumnBuilder();
}

class AnyColumn extends GenericColumn {
  AnyColumn(List values, Mask naValues, Mask nmValues)
      : super(values, naValues, nmValues);

  List _mkList(int size) => new List(size);

  Column _mkCol(List values, Mask naValues, Mask nmValues) =>
      new AnyColumn(values, naValues, nmValues);

  ColumnBuilder _mkColBldr() => new AnyColumnBuilder();
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
