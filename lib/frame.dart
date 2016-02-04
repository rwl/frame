library frame;

import 'dart:collection' show ListBase;
import 'package:option/option.dart';

part 'src/cell.dart';
part 'src/column.dart';
part 'src/index.dart';
part 'src/series.dart';
part 'src/frame.dart';

class Tuple2<A, B> {
  final A v1;
  final B v2;
  Tuple2(this.v1, this.v2);
}

class Tuple3<A, B, C> {
  final A v1;
  final B v2;
  final C v3;
  Tuple3(this.v1, this.v2, this.v3);
}
