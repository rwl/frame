import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';
import 'package:quiver/iterables.dart' show range;

import 'data.dart';

class ColumnFilterBenchmark extends BenchmarkBase {
  const ColumnFilterBenchmark() : super("ColumnFilterBenchmark");

  static void main() {
    new ColumnFilterBenchmark().report();
  }

  void run() {}

  void setup() {}

  void teardown() {}

  int dense(FilterData data) =>
      Data.work(data.denseColumn.filter(data.p), data.size);

  int eval(FilterData data) =>
      Data.work(data.evalColumn.filter(data.p), data.size);

  int optimisticMemoized(FilterData data) =>
      Data.work(data.optMemoColumn.filter(data.p), data.size);

  int pessimisticMemoized(FilterData data) =>
      Data.work(data.pesMemoColumn.filter(data.p), data.size);
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
  ColumnFilterBenchmark.main();
}
