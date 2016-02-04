library frame.test.cell;

import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

cellTest() {
  group("Cell", () {
    test("be constructable from Option", () {
      expect(Cell.fromOption(new Some(2)), equals(new Value(2)));
      expect(Cell.fromOption(new None()), equals(NA));
    });

    test("have sane comparison", () {
      var order = Cell.cellOrder; //[Int]
      expect(order.compare(new Value(1), new Value(1)), equals(0));
      expect(order.compare(new Value(2), new Value(1)), equals(1));
      expect(order.compare(new Value(1), new Value(2)), equals(-1));
      expect(order.compare(NA, NA), equals(0));
      expect(order.compare(NM, NM), equals(0));
      expect(order.compare(NA, NM), equals(-1));
      expect(order.compare(NA, new Value(1)), equals(-1));
      expect(order.compare(new Value(1), NA), equals(1));
      expect(order.compare(NM, NA), equals(1));
      expect(order.compare(new Value(1), NM), equals(1));
    });

    test("have sane equality", () {
      expect(NA, equals(NA));
      expect(NA, equals(new Value(NA)));
      expect(new Value(NA), equals(NA));
      expect(NM, equals(NM));
      expect(NM, equals(new Value(NM)));
      expect(new Value(NM), equals(NM));
      expect(new Value(2), equals(new Value(2)));
    });

    tnie() {
      throw new UnimplementedError();
    }

    test("fold correctly", () {
      expect(new Value(1).fold(tnie, tnie, (x) => x + 1), equals(2));
      expect(NA.fold(42, tnie, (x) => tnie()), equals(42));
      expect(NM.fold(tnie, 42, (x) => tnie()), equals(42));
    });

    test("return value for getOrElse when Value", () {
      expect(new Value(42).getOrElse(tnie), equals(42));
    });

    test("return default for getOrElse when NA/NM", () {
      expect(NA.getOrElse(42), equals(42));
      expect(NM.getOrElse(42), equals(42));
    });

    test("be mappable", () {
      expect(new Value(7).map((v) => v * 3), equals(new Value(21)));
      expect((NA as Cell<int>).map((v) => v * 3), equals(NA));
      expect((NM as Cell<int>).map((v) => v * 3), equals(NM));
    });

    test("be flatMappable", () {
      expect(new Value(7).flatMap((v) => NA), equals(NA));
      expect(
          new Value(7).flatMap((x) => new Value(x * 3)), equals(new Value(21)));
      expect(NA.flatMap((v) => NM), equals(NA));
      expect(NM.flatMap((v) => NA), equals(NM));
    });

    test("filter changes cell to NA when false", () {
      expect(new Value(2).filter((v) => v == 2), equals(new Value(2)));
      expect(new Value(2).filter((v) => v != 2), equals(NA));
      expect(NA.filter((v) => v == 2), equals(NA));
      expect(NM.filter((v) => v == 2), equals(NM));
    });

    test("zipMap values", () {
      expect(new Value(1).zipMap(new Value(3.0), (a, b) => a + b),
          equals(new Value(4.0)));
    });

    test("zipMap NAs", () {
      expect((NA as Cell<int>).zipMap((NA as Cell<int>), (a, b) => a + b),
          equals(NA));

      expect(
          new Value(2).zipMap((NA as Cell<int>), (a, b) => a + b), equals(NA));
      expect(
          (NA as Cell<int>).zipMap(new Value(2), (a, b) => a + b), equals(NA));

      expect((NA as Cell<int>).zipMap((NM as Cell<int>), (a, b) => a + b),
          equals(NA));
      expect((NM as Cell<int>).zipMap((NA as Cell<int>), (a, b) => a + b),
          equals(NA));
    });

    test("zipMap NMs", () {
      expect((NM as Cell<int>).zipMap((NM as Cell<int>), (a, b) => a + b),
          equals(NM));

      expect(
          new Value(2).zipMap((NM as Cell<int>), (a, b) => a + b), equals(NM));
      expect(
          (NM as Cell<int>).zipMap(new Value(2), (a, b) => a + b), equals(NM));
    });
  });
}
