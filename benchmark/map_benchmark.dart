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

class OptMemo10MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  OptMemo10MapWithSmallReadBenchmark() : super('optimisticMemoized10');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size ~/ 10);
}

class PesMemo10MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  PesMemo10MapWithSmallReadBenchmark() : super('pessimisticMemoized10');

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

class OptMemo1MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  OptMemo1MapWithSmallReadBenchmark() : super('optimisticMemoized1');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size ~/ 100);
}

class PesMemo1MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  PesMemo1MapWithSmallReadBenchmark() : super('pessimisticMemoized1');

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

class OptMemo50MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  OptMemo50MapWithSmallReadBenchmark() : super('optimisticMemoized50');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size ~/ 2);
}

class PesMemo50MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  PesMemo50MapWithSmallReadBenchmark() : super('pessimisticMemoized50');

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

class OptMemo0MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  OptMemo0MapWithSmallReadBenchmark() : super('optimisticMemoized0');

  run() => data.optMemoColumn.map(data.f);
}

class PesMemo0MapWithSmallReadBenchmark extends MapWithSmallReadBenchmark {
  PesMemo0MapWithSmallReadBenchmark() : super('pessimisticMemoized0');

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

class OptMemoColumnMapBenchmark extends ColumnMapBenchmark {
  OptMemoColumnMapBenchmark() : super('optimisticMemoized');

  run() => Data.work(data.optMemoColumn.map(data.f), data.size);
}

class PesMemoColumnMapBenchmark extends ColumnMapBenchmark {
  PesMemoColumnMapBenchmark() : super('pessimisticMemoized');

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

class OptMemoManyReadBenchmark extends ManyReadBenchmark {
  OptMemoManyReadBenchmark() : super('optimisticMemoized');

  run() {
    var col = data.optMemoColumn.map(data.f);
    Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size) +
        Data.work(col, data.size);
  }
}

class PesMemoManyReadBenchmark extends ManyReadBenchmark {
  PesMemoManyReadBenchmark() : super('pessimisticMemoized');

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
  new OptMemo10MapWithSmallReadBenchmark().report();
  new PesMemo10MapWithSmallReadBenchmark().report();
  new Dense1MapWithSmallReadBenchmark().report();
  new Eval1MapWithSmallReadBenchmark().report();
  new OptMemo1MapWithSmallReadBenchmark().report();
  new PesMemo1MapWithSmallReadBenchmark().report();
  new Dense50MapWithSmallReadBenchmark().report();
  new Eval50MapWithSmallReadBenchmark().report();
  new OptMemo50MapWithSmallReadBenchmark().report();
  new PesMemo50MapWithSmallReadBenchmark().report();
  new Dense0MapWithSmallReadBenchmark().report();
  new Eval0MapWithSmallReadBenchmark().report();
  new OptMemo0MapWithSmallReadBenchmark().report();
  new PesMemo0MapWithSmallReadBenchmark().report();

  new DenseColumnMapBenchmark().report();
  new EvalColumnMapBenchmark().report();
  new OptMemoColumnMapBenchmark().report();
  new PesMemoColumnMapBenchmark().report();

  new DenseManyReadBenchmark().report();
  new EvalManyReadBenchmark().report();
  new OptMemoManyReadBenchmark().report();
  new PesMemoManyReadBenchmark().report();

  new Dense5ManyMapBenchmark().report();
  new Dense10ManyMapBenchmark().report();
  new Dense20ManyMapBenchmark().report();
  new Dense40ManyMapBenchmark().report();
  new Eval5ManyMapBenchmark().report();
  new Eval10ManyMapBenchmark().report();
  new Eval20ManyMapBenchmark().report();
  new Eval40ManyMapBenchmark().report();
}
