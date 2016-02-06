library frame;

import 'dart:math' as math;
import 'dart:collection' show ListBase;
import 'dart:typed_data' show Uint32List;

import 'package:option/option.dart';
import 'package:duty/match.dart' show PartialFunction;
import 'package:quiver/core.dart' show hash2, hash3;

part 'src/cell.dart';
part 'src/column.dart';
part 'src/index.dart';
part 'src/series.dart';
part 'src/frame.dart';

part 'src/column/mask.dart';
part 'src/column/dense_column.dart';
part 'src/column/eval_column.dart';

const int MIN_INT = -9007199254740991; //-2147483648;
const int MAX_INT = 9007199254740991; //2147483647;

class Tuple2<T1, T2> {
  final T1 v1;
  final T2 v2;

  const Tuple2(this.v1, this.v2);

  bool operator ==(other) {
    return other is Tuple2 && v1 == other.v1 && v2 == other.v2;
  }

  String toString() => "Tuple($v1, $v2)";

  int get hashCode => hash2(v1, v2);
}

class Tuple3<T1, T2, T3> extends Tuple2<T1, T2> {
  final T3 v3;

  const Tuple3(T1 value1, T2 value2, this.v3) : super(value1, value2);

  bool operator ==(other) =>
      other is Tuple3 && v1 == other.v1 && v2 == other.v2 && v3 == other.v3;

  String toString() => "Tuple3($v1, $v2, $v3)";

  int get hashCode => hash3(v1, v2, v3);
}
