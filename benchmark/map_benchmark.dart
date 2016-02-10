import 'dart:math' show Random;

import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';
import 'package:quiver/iterables.dart' show range;

import 'data.dart';

abstract class MapWithSmallReadBenchmark extends BenchmarkBase {
  MapData data;

  MapWithSmallReadBenchmark(String name)
      : super('MapWithSmallReadBenchmark.$name');

  void setup() {
    data = new MapData();
  }
}

class Dense10MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Dense10MapWithSmallReadBenchmark() : super('dense10');

  run() => Data.work(data.denseColumn.map(data.f), data.size ~/ 10);
}

class Eval10MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Eval10MapWithSmallReadBenchmark() : super('eval10');

  run() => Data.work(data.evalColumn.map(data.f), data.size ~/ 10);
}

class OptimisticMemoized10MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  OptimisticMemoized10MapWithSmallReadBenchmark()
      : super('optimisticMemoized10');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size ~/ 10);
}

class PessimisticMemoized10MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  PessimisticMemoized10MapWithSmallReadBenchmark()
      : super('pessimisticMemoized10');

  run() => Data.work(data.pesMemoColumn.map(data.f), data.size ~/ 10);
}

class Dense1MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Dense1MapWithSmallReadBenchmark() : super('dense1');

  run() => Data.work(data.denseColumn.map(data.f), data.size ~/ 100);
}

class Eval1MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Eval1MapWithSmallReadBenchmark() : super('eval1');

  run() => Data.work(data.evalColumn.map(data.f), data.size ~/ 100);
}

class OptimisticMemoized1MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  OptimisticMemoized1MapWithSmallReadBenchmark() : super('optimisticMemoized1');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size ~/ 100);
}

class PessimisticMemoized1MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  PessimisticMemoized1MapWithSmallReadBenchmark()
      : super('pessimisticMemoized1');

  run() => Data.work(data.pesMemoColumn.map(data.f), data.size ~/ 100);
}

class Dense50MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Dense50MapWithSmallReadBenchmark() : super('dense50');

  run() => Data.work(data.denseColumn.map(data.f), data.size ~/ 2);
}

class Eval50MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Eval50MapWithSmallReadBenchmark() : super('');

  run() => Data.work(data.evalColumn.map(data.f), data.size ~/ 2);
}

class OptimisticMemoized50MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  OptimisticMemoized50MapWithSmallReadBenchmark()
      : super('optimisticMemoized50');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size ~/ 2);
}

class PessimisticMemoized50MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  PessimisticMemoized50MapWithSmallReadBenchmark()
      : super('pessimisticMemoized50');

  run() => Data.work(data.pesMemoColumn.map(data.f), data.size ~/ 2);
}

class Dense0MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Dense0MapWithSmallReadBenchmark() : super('dense0');

  run() => data.denseColumn.map(data.f);
}

class Eval0MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  Eval0MapWithSmallReadBenchmark() : super('eval0');

  run() => data.evalColumn.map(data.f);
}

class OptimisticMemoized0MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  OptimisticMemoized0MapWithSmallReadBenchmark() : super('optimisticMemoized0');

  run() => data.optMemoColumn.map(data.f);
}

class PessimisticMemoized0MapWithSmallReadBenchmark
    extends MapWithSmallReadBenchmark {
  PessimisticMemoized0MapWithSmallReadBenchmark()
      : super('pessimisticMemoized0');

  run() => data.pesMemoColumn.map(data.f);
}

abstract class ColumnMapBenchmark extends BenchmarkBase {
  MapData data;

  ColumnMapBenchmark(String name) : super(name);

  void setup() {
    data = new MapData();
  }
}

class DenseColumnMapBenchmark extends ColumnMapBenchmark {
  DenseColumnMapBenchmark() : super('dense');

  run() => Data.work(data.denseColumn.map(data.f), data.size);
}

class EvalColumnMapBenchmark extends ColumnMapBenchmark {
  EvalColumnMapBenchmark() : super('eval');

  run() => Data.work(data.evalColumn.map(data.f), data.size);
}

class OptimisticMemoizedColumnMapBenchmark extends ColumnMapBenchmark {
  OptimisticMemoizedColumnMapBenchmark() : super('optimisticMemoized');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size);
}

class PessimisticMemoizedColumnMapBenchmark extends ColumnMapBenchmark {
  PessimisticMemoizedColumnMapBenchmark() : super('pessimisticMemoized');

  run() => Data.work(data.pesMemoColumn.map(data.f), data.size);
}

abstract class ManyReadBenchmark extends BenchmarkBase {
  MapData data;

  ManyReadBenchmark(String name) : super(name);

  void setup() {
    data = new MapData();
  }
}

class DenseManyReadBenchmark extends ManyReadBenchmark {
  DenseManyReadBenchmark() : super('dense');

  run() {
    var col = data.denseColumn.map(data.f);
    Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size);
  }
}

class EvalManyReadBenchmark extends ManyReadBenchmark {
  EvalManyReadBenchmark() : super('eval');

  run() {
    var col = data.evalColumn.map(data.f);
    Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size);
  }
}

class OptimisticMemoizedManyReadBenchmark extends ManyReadBenchmark {
  OptimisticMemoizedManyReadBenchmark() : super('optimisticMemoized');

  run() {
    var col = data.optMemoColumn.map(data.f);
    Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size);
  }
}

class PessimisticMemoizedManyReadBenchmark extends ManyReadBenchmark {
  PessimisticMemoizedManyReadBenchmark() : super('pessimisticMemoized');

  run() {
    var col = data.pesMemoColumn.map(data.f);
    Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size);
  }
}

abstract class ManyMapBenchmark extends BenchmarkBase {
  MapData data;

  ManyMapBenchmark(String name) : super(name);

  Column<int> mult(Column<int> col, int f(int i), int i) =>
      (i == 0) ? col : mult(col.map(f), f, i - 1);
}

class Dense5ManyMapBenchmark extends ManyMapBenchmark {
  Dense5ManyMapBenchmark() : super('dense5');

  run() => Data.work(mult(data.denseColumn, data.f, 5), data.size);
}

class Dense10ManyMapBenchmark extends ManyMapBenchmark {
  Dense10ManyMapBenchmark() : super('dense10');

  run() => Data.work(mult(data.denseColumn, data.f, 10), data.size);
}

class Dense20ManyMapBenchmark extends ManyMapBenchmark {
  Dense20ManyMapBenchmark() : super('dense20');

  run() => Data.work(mult(data.denseColumn, data.f, 20), data.size);
}

class Dense40ManyMapBenchmark extends ManyMapBenchmark {
  Dense40ManyMapBenchmark() : super('dense40');

  run() => Data.work(mult(data.denseColumn, data.f, 40), data.size);
}

class Eval5ManyMapBenchmark extends ManyMapBenchmark {
  Eval5ManyMapBenchmark() : super('eval5');

  run() => Data.work(mult(data.evalColumn, data.f, 5), data.size);
}

class Eval10ManyMapBenchmark extends ManyMapBenchmark {
  Eval10ManyMapBenchmark() : super('eval10');

  run() => Data.work(mult(data.evalColumn, data.f, 10), data.size);
}

class Eval20ManyMapBenchmark extends ManyMapBenchmark {
  Eval20ManyMapBenchmark() : super('eval20');

  run() => Data.work(mult(data.evalColumn, data.f, 20), data.size);
}

class Eval40ManyMapBenchmark extends ManyMapBenchmark {
  Eval40ManyMapBenchmark() : super('eval40');

  run() => Data.work(mult(data.evalColumn, data.f, 40), data.size);
}

class MapData extends Data {
  int f(int x) => x + 1;

  Column<int> denseColumn;
  Column<int> evalColumn;
  Column<int> optMemoColumn;
  Column<int> pesMemoColumn;

  MapData() {
    denseColumn = new Column.dense(range(0, size));
    evalColumn = new Column.eval((row) => new Value(row));
    optMemoColumn = evalColumn.memoize(true);
    pesMemoColumn = evalColumn.memoize(false);
  }
}

main() {
  new Dense10MapWithSmallReadBenchmark().report();
  new Eval10MapWithSmallReadBenchmark().report();
  new OptimisticMemoized10MapWithSmallReadBenchmark().report();
  new PessimisticMemoized10MapWithSmallReadBenchmark().report();
  new Dense1MapWithSmallReadBenchmark().report();
  new Eval1MapWithSmallReadBenchmark().report();
  new OptimisticMemoized1MapWithSmallReadBenchmark().report();
  new PessimisticMemoized1MapWithSmallReadBenchmark().report();
  new Dense50MapWithSmallReadBenchmark().report();
  new Eval50MapWithSmallReadBenchmark().report();
  new OptimisticMemoized50MapWithSmallReadBenchmark().report();
  new PessimisticMemoized50MapWithSmallReadBenchmark().report();
  new Dense0MapWithSmallReadBenchmark().report();
  new Eval0MapWithSmallReadBenchmark().report();
  new OptimisticMemoized0MapWithSmallReadBenchmark().report();
  new PessimisticMemoized0MapWithSmallReadBenchmark().report();

  new DenseColumnMapBenchmark().report();
  new EvalColumnMapBenchmark().report();
  new OptimisticMemoizedColumnMapBenchmark().report();
  new PessimisticMemoizedColumnMapBenchmark().report();

  new DenseManyReadBenchmark().report();
  new EvalManyReadBenchmark().report();
  new OptimisticMemoizedManyReadBenchmark().report();
  new PessimisticMemoizedManyReadBenchmark().report();

  new Dense5ManyMapBenchmark().report();
  new Dense10ManyMapBenchmark().report();
  new Dense20ManyMapBenchmark().report();
  new Dense40ManyMapBenchmark().report();
  new Eval5ManyMapBenchmark().report();
  new Eval10ManyMapBenchmark().report();
  new Eval20ManyMapBenchmark().report();
  new Eval40ManyMapBenchmark().report();
}
