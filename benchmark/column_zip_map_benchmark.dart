import 'package:frame/frame.dart';
import 'package:benchmark_harness/benchmark_harness.dart';

import 'data.dart';

class DenseColumnZipMapBenchmark extends BenchmarkBase {
  const DenseColumnZipMapBenchmark() : super("DenseColumnZipMapBenchmark");

  static void main() {
    new DenseColumnZipMapBenchmark().report();
  }

  void run() {}

  void setup() {}

  void teardown() {}

  zipMapArray(DenseZipMapData data) {
    var xs = data.data0;
    var ys = data.data1;
    var len = math.min(xs.length, ys.length);
    var zs = new List<double>(len);
    var i = 0;
    while (i < xs.length && i < ys.length) {
      zs[i] = xs[i] * ys[i];
      i += 1;
    }
    return zs;
  }

  zipMappedMaskedArray(DenseZipMapData data) {
    var xs = data.data0;
    var ys = data.data1;
    var len = math.min(xs.length, ys.length);
    var na = data.na0 | data.na1;
    var nm = (data.nm0 | data.nm1).filter((i) => i < len && !na[i]);
    var zs = new List<double>(len);
    var i = 0;
    while (i < xs.length && i < ys.length) {
      if (!(na[i] || nm[i])) {
        zs[i] = xs[i] * ys[i];
      }
      i += 1;
    }
    return zs;
  }

  zipMapColumn(DenseZipMapData data) =>
      data.col0.zipMap(data.col1, (a, b) => a * b);

  zipMapSeries(DenseZipMapData data) =>
      data.series0.zipMap(data.series1, (a, b) => a * b);
}

class DenseZipMapData {
  List<double> data0, data1;
  Mask na0, nm0, na1, nm1;
  Column<double> col0, col1;
  Series<int, double> series0, series1;

  DenseZipMapData() {
    var size = 1000;
    var rng = new Random(42);

    data0 = new List.generate(size, (_) => rng.nextDouble());
    na0 = Data.mask(rng, size, 0.1);
    nm0 = Data.mask(rng, size, 0.01);
    col0 = Column.dense(data0, na0, nm0);
    series0 = new Series(new Index.fromKeys(range(1, size + 1)), col0);

    data1 = new List.generate(size, (_) => rng.nextDouble());
    na1 = Data.mask(rng, size, 0.1);
    nm1 = Data.mask(rng, size, 0.01);
    col1 = new Column.dense(data1, na1, nm1);
    series1 = new Series(new Index.fromKeys(range(1, size + 1)), col1);
  }
}

main() {
  DenseColumnZipMapBenchmark.main();
}
