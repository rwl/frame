import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';
import 'package:quiver/iterables.dart' show range;

import 'data.dart';

abstract class ColumnReindexBenchmark extends BenchmarkBase {
  ReindexData data;

  ColumnReindexBenchmark(String name) : super('ColumnReindexBenchmark.$name');

  void setup() {
    data = new ReindexData();
  }
}

class DenseColumnReindexBenchmark extends ColumnReindexBenchmark {
  DenseColumnReindexBenchmark() : super('dense');

  run() => Data.work(data.denseColumn.reindex(data.indices), data.size);
}

class EvalColumnReindexBenchmark extends ColumnReindexBenchmark {
  EvalColumnReindexBenchmark() : super('eval');

  run() => Data.work(data.evalColumn.reindex(data.indices), data.size);
}

class OptMemoColumnReindexBenchmark extends ColumnReindexBenchmark {
  OptMemoColumnReindexBenchmark() : super('optimisticMemoized');

  run() => Data.work(data.optMemoColumn.reindex(data.indices), data.size);
}

class PesMemoColumnReindexBenchmark extends ColumnReindexBenchmark {
  PesMemoColumnReindexBenchmark() : super('pessimisticMemoized');

  run() => Data.work(data.pesMemoColumn.reindex(data.indices), data.size);
}

class ReindexData extends Data {
  List<int> indices;
  Column<int> denseColumn, evalColumn, optMemoColumn, pesMemoColumn;

  ReindexData() {
    indices = rng.shuffle(range(size).toList()).toArray();

    denseColumn = new Column.dense(range(size));
    evalColumn = new Column.eval((row) => new Value(row));
    optMemoColumn = evalColumn.memoize(true);
    pesMemoColumn = evalColumn.memoize(false);
  }
}

main() {
  new DenseColumnReindexBenchmark().report();
  new EvalColumnReindexBenchmark().report();
  new OptMemoColumnReindexBenchmark().report();
  new PesMemoColumnReindexBenchmark().report();
}
