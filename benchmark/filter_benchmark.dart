import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';
import 'package:quiver/iterables.dart' show range;

import 'data.dart';

abstract class ColumnFilterBenchmark extends BenchmarkBase {
  FilterData data;

  ColumnFilterBenchmark(String name) : super("ColumnFilterBenchmark.$name");

  void setup() {
    data = new FilterData();
  }
}

class DenseColumnFilterBenchmark extends ColumnFilterBenchmark {
  DenseColumnFilterBenchmark() : super('dense');

  run() => Data.work(data.denseColumn.filter(data.p), data.size);
}

class EvalColumnFilterBenchmark extends ColumnFilterBenchmark {
  EvalColumnFilterBenchmark() : super('eval');

  run() => Data.work(data.evalColumn.filter(data.p), data.size);
}

class OptimisticMemoizedColumnFilterBenchmark extends ColumnFilterBenchmark {
  OptimisticMemoizedColumnFilterBenchmark() : super('optimisticMemoized');

  run() => Data.work(data.optMemoColumn.filter(data.p), data.size);
}

class PessimisticMemoizedColumnFilterBenchmark extends ColumnFilterBenchmark {
  PessimisticMemoizedColumnFilterBenchmark() : super('pessimisticMemoized');

  run() => Data.work(data.pesMemoColumn.filter(data.p), data.size);
}

class FilterData extends Data {
  bool p(int x) => x % 2 == 0;

  Column<int> denseColumn, evalColumn, optMemoColumn, pesMemoColumn;

  FilterData() {
    denseColumn = new Column.dense(range(size));
    evalColumn = new Column.eval((row) => new Value(row));
    optMemoColumn = evalColumn.memoize(true);
    pesMemoColumn = evalColumn.memoize(false);
  }
}

main() {
  new DenseColumnFilterBenchmark().report();
  new EvalColumnFilterBenchmark().report();
  new OptimisticMemoizedColumnFilterBenchmark().report();
  new PessimisticMemoizedColumnFilterBenchmark().report();
}
