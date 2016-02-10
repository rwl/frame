library frame.test.untyped_column;

import 'package:test/test.dart';
import 'package:frame/frame.dart';

untypedColumnSpec() {
  group("ConcatColumn should", () {
    test("be stable after reindex", () {
      var col1 = new TypedColumn(new Column.dense([1, 2]));
      var col2 = new TypedColumn(new Column.dense([3, 4]));
      var col3 = new ConcatColumn(col1, col2, 2);
      var col = col3.reindex([0, 1, 2, 3]).cast[Int];
      expect(range(5).map((i) => col(i)),
          equals([new Value(1), new Value(2), new Value(3), new Value(4), NA]));
    });
  });

  group("cast should", () {
    test("return NMs when values don't make sense", () {
      var col0 = new TypedColumn(
              new Column.fromValues([new Value(42), NA, NM, new Value(32)]))
          .cast[String];
      expect(col0(0), equals(NM));
      expect(col0(1), equals(NA));
      expect(col0(2), equals(NM));
      expect(col0(3), equals(NM));
      expect(col0(4), equals(NA));

      var col1 = new TypedColumn(
              new Column.fromValues([new Value("x"), NA, NM, new Value("y")]))
          .cast[Double];
      expect(col1(0), equals(NM));
      expect(col1(1), equals(NA));
      expect(col1(2), equals(NM));
      expect(col1(3), equals(NM));
      expect(col1(4), equals(NA));
    });
  });
}
