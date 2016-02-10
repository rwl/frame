import 'dart:math' show Random;

import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';
import 'package:quiver/iterables.dart' show range;

import 'data.dart' as dat;

abstract class SeriesZipMapBenchmark extends BenchmarkBase {
  Data data;

  SeriesZipMapBenchmark(String name) : super('SeriesZipMapBenchmark.$name');

  setup() {
    data = new Data();
  }
}

class JoinIndicesBenchmark extends SeriesZipMapBenchmark {
  JoinIndicesBenchmark() : super('joinIndices');

  void run() {
    var joiner = new Joiner<int>(Join.Inner);
    new Index.cogroup(data.idx0, data.idx1, joiner).result();
  }
}

class ReindexColumnsBenchmark extends SeriesZipMapBenchmark {
  ReindexColumnsBenchmark() : super('reindexColumns');

  run() {
    var col0 = data.col0.reindex(data.indices0);
    var col1 = data.col1.reindex(data.indices1);
    return new Tuple2(col0, col1);
  }
}

class ZipMapColumnBenchmark extends SeriesZipMapBenchmark {
  ZipMapColumnBenchmark() : super('zipMapColumn');

  run() => data.col0.zipMap(data.col1, (a, b) => a * b);
}

class MakeIndexBenchmark extends SeriesZipMapBenchmark {
  MakeIndexBenchmark() : super('makeIndex');

  run() => new Index.ordered(data.indices0);
}

class ZipMapSeriesBenchmark extends SeriesZipMapBenchmark {
  ZipMapSeriesBenchmark() : super('zipMapSeries');

  run() => data.series0.zipMap(data.series1, (a, b) => a * b);
}

class Data {
  List<int> indices0, indices1;
  Index<int> idx0, idx1;
  Column<double> col0, col1;
  Series<int, double> series0, series1;

  Data() {
    var size = 1000;
    var rng = new Random(42);

    indices0 = range(size).toList();
    indices1 = range(size).toList();

    var data0 = new List<double>.generate(size, (_) => rng.nextDouble());
    var na0 = dat.Data.mask(rng, size, 0.1);
    var nm0 = dat.Data.mask(rng, size, 0.01);
    col0 = new Column.dense(data0, na0, nm0);
    idx0 = new Index(indices0);
    series0 = new Series(idx0, col0);

    var data1 = new List<double>.generate(size, (_) => rng.nextDouble());
    var na1 = dat.Data.mask(rng, size, 0.1);
    var nm1 = dat.Data.mask(rng, size, 0.01);
    col1 = new Column.dense(data1, na1, nm1);
    idx1 = new Index(indices1);
    series1 = new Series(Index.fromKeys(range(1, size + 1)), col1);
  }
}

main() {
  new JoinIndicesBenchmark().report();
  new ReindexColumnsBenchmark().report();
  new ZipMapColumnBenchmark().report();
  new MakeIndexBenchmark().report();
  new ZipMapSeriesBenchmark().report();
}
