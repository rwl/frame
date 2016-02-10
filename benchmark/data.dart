library frame.benchmark;

import 'dart:math' show Random;

import 'package:quiver/iterables.dart' show enumerate;
import 'package:frame/frame.dart';

abstract class Data {
  int size = 1000;
  Random rng = new Random(42);

  static int work(Column<int> col, int size) {
    var sum = 0;
    var i = 0;
    while (i < size) {
      sum -= col.apply(i).getOrElse(() => 0);
      i += 1;
    }
    return sum;
  }

  static Mask mask(Random rng, int n, [double p = 0.1]) {
    var r = new List<double>.generate(1000, (_) => rng.nextDouble());
    return new Mask.from(
        enumerate(r).where((iv) => iv.value < p).map((iv) => iv.index));
  }
}
