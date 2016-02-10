import 'dart:math' show Random;

import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';

import 'data.dart';

abstract class DenseColumnMapBenchmark extends BenchmarkBase {
  DoubleData data;

  DenseColumnMapBenchmark(String name) : super(name);

  void setup() {
    data = new DoubleData();
  }
}

class SquareListBenchmark extends DenseColumnMapBenchmark {
  SquareListBenchmark() : super('SquareList');

  void run() {
    var xs = data.data;
    var ys = new List<double>(xs.length);
    var i = 0;
    while (i < xs.length) {
      var x = xs[i];
      ys[i] = x * x;
      i += 1;
    }
    return ys;
  }
}

class SquareMaskedListBenchmark extends DenseColumnMapBenchmark {
  SquareMaskedListBenchmark() : super('SquareMaskedList');

  void run() {
    var xs = data.data;
    var ys = new List<double>(xs.length);
    var i = 0;
    while (i < xs.length) {
      if (!(data.na[i] || data.nm[i])) {
        var x = xs[i];
        ys[i] = x * x;
      }
      i += 1;
    }
    return ys;
  }
}

class SquareColumnBenchmark extends DenseColumnMapBenchmark {
  SquareColumnBenchmark() : super('SquareColumn');

  void run() {
    data.col.map((x) => x * x);
  }
}

class SquareBitSetMaskedListBenchmark extends DenseColumnMapBenchmark {
  SquareBitSetMaskedListBenchmark() : super('SquareBitSetMaskedList');

  void run() {
    var xs = data.data;
    var ys = new List<double>(xs.length);
    var i = 0;
    while (i < xs.length) {
      if (!(data.na0[i] || data.nm0[i])) {
        var x = xs[i];
        ys[i] = x * x;
      }
      i += 1;
    }
    return ys;
  }
}

class DoubleData {
  List<double> data;
  Mask na, nm;
  BitSet na0, nm0;
  Column<double> col;

  DoubleData() {
    var size = 1000;
    var rng = new Random(42);
    data = new List.generate(size, rng.nextDouble());
    na = Data.mask(rng, size, 0.1);
    nm = Data.mask(rng, size, 0.01);
    col = new Column.dense(data, na, nm);

    na0 = na.toBitSet();
    nm0 = nm.toBitSet();
  }
}

main() {
  new SquareListBenchmark().report();
  new SquareMaskedListBenchmark().report();
  new SquareColumnBenchmark().report();
  new SquareBitSetMaskedListBenchmark().report();
}
