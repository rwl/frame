library frame.test.column;

import 'package:test/test.dart';
import 'package:frame/frame.dart';
import 'package:option/option.dart';
import 'package:quiver/iterables.dart' show range;

import 'generators.dart' as gen;

columnTest() {
  List<Cell> slice(Column col, Iterable<int> indices) =>
      indices.toList().map((i) => col.apply(i)).toList();

//    Column<A> mkEval(Column<A> col) => new Column.eval((row) => col.apply(row));
//
//    var cg = new gen.CellGenerator<A>(rand);
//
//    Column<A> genColumn() {
//      var cellValues =
//          new List<Cell<A>>.generate(gen.maxl, (_) => cg.genCell(2, 1, 1));
//      var dense = gen.r.nextBool();
//      var col = new Column.fromCells(cellValues);
//      return dense ? col : mkEval(col);
//    }
//
//    Column<A> col;
//    setUp(() {
//      col = genColumn();
//    });

  group("Column construction", () {
    test("wrap arrays", () {
      var col = new Column.dense([1, 2, 3]);
      expect(slice(col, [-1, 0, 1, 2, 3]),
          equals([NA, new Value(1), new Value(2), new Value(3), NA]));
    });

    test("constructable from Cells", () {
      var col = new Column.fromCells([NM, NA, new Value("a"), NM]);
      expect(slice(col, [-1, 0, 1, 2, 3, 4]),
          equals([NA, NM, NA, new Value("a"), NM, NA]));
    });

    test("wrap row function", () {
      var col = new Column.eval((row) => new Value(-row));
      expect(col.apply(0), equals(new Value(0)));
      //expect(col.apply(MIN_INT), equals(new Value(MIN_INT)));
      expect(col.apply(MAX_INT), equals(new Value(-MAX_INT)));
      expect(col.apply(5), equals(new Value(-5)));
    });

    test("empty is empty", () {
      var col = new Column.empty /*[String]*/ ();
      expect(
          slice(col, [MIN_INT, 0, MAX_INT, -1, 1, 200]).every((c) => c == NA),
          isTrue);
    });
  });

  group("memo columns", () {
    test("only evaluate values at most once", () {
      var counter = 0;
      var col = new Column.eval((row) {
        counter += 1;
        return new Value(row);
      }).memoize();
      col.apply(0);
      col.apply(0);
      col.apply(1);
      col.apply(0);
      col.apply(0);
      col.apply(1);
      col.apply(2);
      col.apply(1);
      col.apply(0);
      expect(counter, equals(3));
    });
  });

  test("orElse with longer right side and NM", () {
    var b = new Column<int>.fromCells([]);
    var c = new Column<int>.fromCells([NA, NM, NM]);
    expect(b.orElse(c).apply(1), equals(NM));
  });

//  group("Monoid[Column<A>]", () {
//    "left identity" in check { (col0: Column[Int], indices: List[Int]) =>
//      val col1 = Monoid[Column[Int]].id orElse col0
//      indices.map(col0(_)) must_== indices.map(col1(_))
//    }
//
//    "right identity" in check { (col0: Column[Int], indices: List[Int]) =>
//      val col1 = col0 orElse Monoid[Column[Int]].id
//      indices.map(col0(_)) must_== indices.map(col1(_))
//    }
//
//    "associative" in check4NoShrink { (a: Column[Int], b: Column[Int], c: Column[Int], indices: List[Int]) =>
//      val col0 = ((a orElse b) orElse c)
//      val col1 = (a orElse (b orElse c))
//      indices.map(col0(_)) must_== indices.map(col1(_))
//    }
//  }
}

baseColumnSpec(Column mkCol(Iterable<Cell> cells)) {
  List<Cell> slice(Column col, Iterable<int> rows) {
    return rows.map((i) => col.apply(i)).toList();
  }

//  Gen<Column<A>> genColumn(Gen<A> gen) {
//    for {
//      cellValues <- Gen.listOf(CellGenerators.genCell(gen, (2, 1, 1)))
//    } yield mkCol(cellValues: _*)
//  }

//  Gen<Mask> genMask() {
//    for {
//      rows0 <- arbitrary[List[Int]]
//      rows = rows0.map(_ & 0xFF)
//    } yield Mask(rows: _*)
//  }

  Column<int> col;
  List<int> indices, rows;
  int row;
  Mask mask;
//    setUp(() {
//      column = genColumn();
//      mask = genMask();
//    });

  group("foldRow", () {
    test("return value for Value row", () {
      var col = mkCol([NA, new Value(42)]);
      expect(col.foldRow(1, 0, 0, (a) => 2 + a), equals(44));
    });

    test("return NA value for NA row", () {
      var col = mkCol([NA, NA]);
      expect(col.foldRow(1, true, false, (_) => false), isTrue);
    });

    test("return NM value for NM row", () {
      var col = mkCol([NA, NM]);
      expect(col.foldRow(1, false, true, (_) => false), isTrue);
    });

//      test("fold rows of columns", () {
//      (col: Column[Int], indices: List[Int]) =>
//      var expected = indices.map((i) => col(i)).map {
//        case Value(a) => a.toString
//        case NA => "NA"
//        case NM => "NM"
//      }
//        var actual = indices.map((i) => col.foldRow(i, "NA", "NM", i.toString));
//        expect(actual, equals(expected));
//      });
  });

  group("foreach", () {
    test("bail early on NM values", () {
      var col = mkCol([new Value(1), new Value(2), NM, new Value(4)]);
      col.forEach(0, 5, (n) => n, (i, n) {
        if (n == 4) {
          fail("n == 4");
        }
      });
    });

    test("skip NA values", () {
      var col = mkCol([new Value(1), NA, new Value(2), NA, new Value(3)]);
      var bldr = [];
      col.forEach(0, 5, (n) => 4 - n, (i, n) {
        bldr.add(n);
      });
      expect(bldr, equals([3, 2, 1]));
    });

    test("work with functions that can't be inlined", () {
      var col = mkCol([new Value(1), NA, new Value(2), NA, new Value(3)]);
      var bldr = <int>[];
      void f(int i, n) => bldr.add(n);
      col.forEach(0, 5, (n) => 4 - n, f);
      expect(bldr, [3, 2, 1]);
    });
  });

  group("map", () {
    test("map each cell in the column", () {
      String f(int n) => (n * 23).toString();
      var col0 = col.map(f);
      expect(indices.map((i) => col.apply(i)).map((c) => c.map(f)),
          equals(indices.map((i) => col0(i))));
    });
  });

  group("flatMap", () {
    test("flatMap each cell in the column", () {
      Cell<String> f(int n) {
        if (n % 3 == 0) {
          return new Value((n * 23).toString());
        } else if (n % 3 == 1) {
          return NA;
        } else {
          return NM;
        }
      }
      ;
      var col0 = col.flatMap(f);
      expect(indices.map((i) => col.apply(i)).map((c) => c.flatMap(f)),
          equals(indices.map((i) => col0(i))));
    });
  });

  group("reindex", () {
    test("return empty column for empty array", () {
      var col = mkCol([new Value(1), new Value(2)]).reindex([]);
      range(-10, 11).map((i) => col(i)).forEach((c) => expect(c, equals(NA)));
    });

    test("reindex rearranges values", () {
      var col = mkCol([new Value(1), new Value(2), new Value(3)]);
      expect(slice(col.reindex([1, 2, 0]), range(3)),
          equals([new Value(2), new Value(3), new Value(1)]));
    });

    test("reindex rearranges NMs", () {
      var col = mkCol([new Value(1), NM, new Value(3), NM]);
      expect(slice(col.reindex([1, 2, 3, 0]), range(4)),
          equals([NM, new Value(3), NM, new Value(1)]));
    });

    test("reindex with negative indices", () {
      var col = mkCol([new Value(1), new Value(2)]);
      expect(slice(col.reindex([-1, 0, -1]), range(3)),
          equals([NA, new Value(1), NA]));
    });
  });

  group("mask", () {
    test("turn all masked rows into NAs", () {
      var masked = col.mask(mask);
      mask.toSet().forEach((i) => expect(masked(i), equals(NA)));
    });

    test("retain unmasked rows as-is", () {
      var validRows = rows.toSet().intersection(mask.toSet());
      var masked = col.mask(mask);
      validRows.forEach((i) => col.apply(i) == masked(i));
    });
  });

  group("setNA", () {
    test("set the specified row to NA", () {
      var col0 = col.setNA(row);
      expect(col0(row), equals(NA));
    });

    test("not modify other rows", () {
      var validRows = rows.toSet()..remove(row);
      var col0 = col.setNA(row);
      validRows.forEach((i) => col.apply(i) == col0.apply(i));
    });
  });

  group("force", () {
    test("return empty column when size is 0", () {
      var empty = col.force(0);
      indices
          .map((i) => empty.apply(i))
          .forEach((cell) => expect(cell, equals(NA)));
    });

    test("not mess with values in range", () {
      /*Byte*/ int blen = gen.r.nextInt(gen.maxl);
      var len = blen.toInt().abs();
      expect(slice(col.force(len), range(len)), equals(slice(col, range(len))));
    });

    test("NA all values out of range", () {
      int len0 = gen.r.nextInt(gen.maxl);
      var len = len0 & 0x7FFFF;
      slice(col.force(len), indices.where((i) => i >= len))
          .forEach((v) => v == NA);
    });
  });

  group("shift", () {
    test("shift all rows", () {
      int rows0 = gen.r.nextInt(gen.maxl);
      var rows = rows0 % 100; // Keep it sane for DenseColumns sake.
      indices = indices
          .where((i) => i < MAX_INT - 100)
          .where((i) => i > MIN_INT + 100);
      var shifted = col.shift(rows);
      expect(indices.map((i) => shifted.apply(i)),
          equals(indices.map((i) => i - rows).map((i) => col.apply(i))));
    });
  });

  group("orElse", () {
    test("be left biased", () {
      var a = mkCol([new Value(0), new Value(1), new Value(2)]);
      var b = mkCol([new Value(0), new Value(-1), new Value(-2)]);
      expect(a.orElse(b).apply(1), equals(new Value(1)));
      expect(a.orElse(b).apply(2), equals(new Value(2)));
      expect(b.orElse(a).apply(1), equals(new Value(-1)));
      expect(b.orElse(a).apply(2), equals(new Value(-2)));
    });

    test("ignore non values", () {
      var a = mkCol([new Value(1), new Value(2), NA, NA, NM, NM, NA, NM]);
      var b = mkCol([NA, NM, NA, NM, NA, NM, new Value(1), new Value(2)]);
      var col = a.orElse(b);

      expect(col.apply(0), equals(new Value(1)));
      expect(col.apply(1), equals(new Value(2)));
      expect(col.apply(2), equals(NA));
      expect(col.apply(3), equals(NM));
      expect(col.apply(4), equals(NM));
      expect(col.apply(5), equals(NM));
      expect(col.apply(6), equals(new Value(1)));
      expect(col.apply(7), equals(new Value(2)));
    });
  });

  group("zipMap", () {
    test("promote all NAs with spec type", () {
      Column<int> na = mkCol([NA]);
      Column<int> nm = mkCol([NM]);
      Column<int> value = mkCol([new Value(1)]);

      expect(na.zipMap(na, (a, b) => a + b).apply(0), equals(NA));
      expect(na.zipMap(nm, (a, b) => a + b).apply(0), equals(NA));
      expect(nm.zipMap(na, (a, b) => a + b).apply(0), equals(NA));
      expect(nm.zipMap(value, (a, b) => a + b).apply(0), equals(NM));
      expect(value.zipMap(na, (a, b) => a + b).apply(0), equals(NA));
    });

    test("promote all NAs with unspec type", () {
      Column<String> na = mkCol([NA]);
      Column<String> nm = mkCol([NM]);
      Column<String> value = mkCol([new Value("x")]);

      expect(na.zipMap(na, (a, b) => a + b).apply(0), equals(NA));
      expect(na.zipMap(nm, (a, b) => a + b).apply(0), equals(NA));
      expect(nm.zipMap(na, (a, b) => a + b).apply(0), equals(NA));
      expect(nm.zipMap(value, (a, b) => a + b).apply(0), equals(NM));
      expect(value.zipMap(na, (a, b) => a + b).apply(0), equals(NA));
    });

    test("NM if both are NM", () {
      Column<String> nm0 = mkCol([NM]);
      Column<int> nm1 = mkCol([NM]);

      expect(nm0.zipMap(nm1, (a, b) => a + b).apply(0), equals(NM));
      expect(nm1.zipMap(nm0, (a, b) => a + b).apply(0), equals(NM));
    });

    test("apply function if both are values", () {
      var col0 = mkCol([new Value(1), NA, NM]);
      var col1 = mkCol([new Value(3.0), new Value(2.0), NA]);
      var col2 = mkCol([NA, new Value("x"), NM]);

      expect(slice(col0.zipMap(col0, (a, b) => a + b), range(4)),
          equals([new Value(2), NA, NM, NA]));
      expect(slice(col0.zipMap(col1, (a, b) => a + b), range(4)),
          equals([new Value(4.0), NA, NA, NA]));
      expect(slice(col0.zipMap(col2, (a, b) => a + b), range(4)),
          equals([NA, NA, NM, NA]));
      expect(slice(col1.zipMap(col0, (a, b) => a + b), range(4)),
          equals([new Value(4.0), NA, NA, NA]));
      expect(slice(col1.zipMap(col1, (a, b) => a + b), range(4)),
          equals([new Value(6.0), new Value(4.0), NA, NA]));
      expect(slice(col1.zipMap(col2, (a, b) => a + b), range(4)),
          equals([NA, new Value("2.0x"), NA, NA]));
      expect(slice(col2.zipMap(col0, (a, b) => a + b), range(4)),
          equals([NA, NA, NM, NA]));
      expect(slice(col2.zipMap(col1, (a, b) => a + b), range(4)),
          equals([NA, new Value("x2.0"), NA, NA]));
      expect(slice(col2.zipMap(col2, (a, b) => a + b), range(4)),
          equals([NA, new Value("xx"), NM, NA]));
    });

    test("conform to same semantics as Cell#zipMap", () {
      Column<int> a;
      Column<double> b;
      var col = a.zipMap(b, (x, y) => x + y);
      expect(
          indices.map((i) => col(i)),
          equals(indices.map(
              (row) => a.apply(row).zipMap(b.apply(row), (x, y) => x + y))));
    });
  });

  group("memoize", () {
    test("calculate values at most once (pessimistic)", () {
      bool hit = false;
      var col0 = col.map((a) {
        hit = true;
        return a;
      }).memoize(false);
      indices.map((i) => col0(i));
      hit = false;
      indices.map((i) => col0(i));
      expect(hit, isFalse);
    });

    test("calculate values at most once (optimistic, no thread contention)",
        () {
      bool hit = false;
      var col0 = col.map((a) {
        hit = true;
        return a;
      }).memoize(true);
      indices.map((i) => col0(i));
      hit = false;
      indices.map((i) => col0(i));
      expect(hit, isFalse);
    });
  });
}

denseColumnTest() {
  Column mkCol(Iterable<Cell> cells) => new Column.fromCells(cells);

  baseColumnSpec(mkCol);

  group("dense columns", () {
    test("manually spec from dense constructor", () {
      expect(
          new Column.dense(["1", "2", "3"]), new isInstanceOf<GenericColumn>());
      expect(new Column.dense([1, 2, 3]), new isInstanceOf<IntColumn>());
      //expect(new Column.dense([1, 2, 3]), new isInstanceOf<LongColumn>());
      expect(
          new Column.dense([1.0, 2.0, 3.0]), new isInstanceOf<DoubleColumn>());
    });

    test("manually spec from default constructor", () {
      expect(new Column.fromCells([NA, new Value("x"), NM]),
          new isInstanceOf<GenericColumn>());
      expect(new Column.fromCells([NA, new Value(1), NM]),
          new isInstanceOf<IntColumn>());
      //expect(new Column.fromCells([NA, new Value(1), NM]),
      //    new isInstanceOf<LongColumn>());
      expect(new Column.fromCells([NA, new Value(1.0), NM]),
          new isInstanceOf<DoubleColumn>());
    });

    test("use AnyColumn when type is not known and not-spec", () {
      Column mkCol(Iterable a) =>
          new Column.fromCells(a.map((aa) => new Value(aa)));
      expect(mkCol(["x", "y"]), new isInstanceOf<AnyColumn>());
    });

    test("use spec col when type is not known but spec", () {
      Column mkCol(Iterable a) =>
          new Column.fromCells(a.map((aa) => new Value(aa)));
      expect(mkCol([1, 2]), new isInstanceOf<IntColumn>());
      //expect(mkCol([1, 2]), new isInstanceOf<LongColumn>());
      expect(mkCol([1.0, 2.0]), new isInstanceOf<DoubleColumn>());
    });

    test("force manual spec through map", () {
      var col = new Column.dense(["1", "2", "3"], new Mask.from([1]));
      expect(col.map((c) => c.toDouble()), new isInstanceOf<DoubleColumn>());
      expect(col.map((c) => c.toInt()), new isInstanceOf<IntColumn>());
      //expect(col.map((c) => c.toLong()), new isInstanceOf<LongColumn>());
    });

    test("force manual spec through flatMap", () {
      var col = new Column.dense(["1", "2", "3"], new Mask.from([1]));
      expect(col.flatMap((n) => new Value(n.toDouble())),
          new isInstanceOf<DoubleColumn>());
      expect(col.flatMap((n) => new Value(n.toInt())),
          new isInstanceOf<IntColumn>());
      //expect(col.flatMap((n) => new Value(n.toLong())),
      //    new isInstanceOf<LongColumn>());
    });

    test("retain manual spec through filter", () {
      expect(new Column.dense([1, 2, 3]).filter((v) => v == 2),
          new isInstanceOf<IntColumn>());
      expect(new Column.dense([1.0, 2.0, 3.0]).filter((v) => v == 2),
          new isInstanceOf<DoubleColumn>());
      //expect(new Column.dense([1, 2, 3]).filter((v) => v == 2),
      //    new isInstanceOf<LongColumn>());
    });

    test("retain manual spec through orElse", () {
      expect(
          new Column.fromCells([new Value(1)])
              .orElse(new Column.fromCells([new Value(2)])),
          new isInstanceOf<IntColumn>());
      //expect(
      //    new Column.fromCells([new Value(1)])
      //        .orElse(new Column.fromCells([new Value(2)])),
      //    new isInstanceOf<LongColumn>());
      expect(
          new Column.fromCells([new Value(1.0)])
              .orElse(new Column.fromCells([new Value(2.0)])),
          new isInstanceOf<DoubleColumn>());
    });

    test("retain manual spec through reindex", () {
      expect(new Column.dense([1, 2, 3]).reindex([2, 1, 0]),
          new isInstanceOf<IntColumn>());
      expect(new Column.dense([1.0, 2.0, 3.0]).reindex([2, 1, 0]),
          new isInstanceOf<DoubleColumn>());
      //expect(new Column.dense([1, 2, 3]).reindex([2, 1, 0]),
      //    new isInstanceOf<LongColumn>());
    });

    test("retain manual spec through force", () {
      expect(
          new Column.dense([1, 2, 3]).force(2), new isInstanceOf<IntColumn>());
      expect(new Column.dense([1.0, 2.0, 3.0]).force(2),
          new isInstanceOf<DoubleColumn>());
      //expect(new Column.dense([1, 2, 3]).force(2),
      //    new isInstanceOf<LongColumn>());
    });

    test("retain manual spec through mask", () {
      var mask = new Mask.from([1]);
      expect(new Column.dense([1, 2, 3]).mask(mask),
          new isInstanceOf<IntColumn>());
      expect(new Column.dense([1.0, 2.0, 3.0]).mask(mask),
          new isInstanceOf<DoubleColumn>());
      //expect(new Column.dense([1, 2, 3]).mask(mask),
      //    new isInstanceOf<LongColumn>());
    });

    test("retain manual spec through setNA", () {
      expect(new Column.dense([1, 2, 3]).setNA(MIN_INT),
          new isInstanceOf<IntColumn>());
      expect(new Column.dense([1.0, 2.0, 3.0]).setNA(MIN_INT),
          new isInstanceOf<DoubleColumn>());
      //expect(new Column.dense([1, 2, 3]).setNA(MIN_INT),
      //    new isInstanceOf<LongColumn>());
    });

    test("setNA should be no-op when row already NA", () {
      //var col = new Column.dense([1, 2, 3]) as IntColumn;
      expect(
          (new Column.dense([1, 2, 3]).setNA(MIN_INT).setNA(3).setNA(MAX_INT)
              as IntColumn).naValues.max(),
          equals(new None()));
      //expect(
      //    (new Column.dense([1, 2, 3]).setNA(MIN_INT).setNA(3).setNA(MAX_INT)
      //        as LongColumn).naValues.max(),
      //    equals(new None()));
      expect(
          (new Column.dense([1.0, 2.0, 3.0])
              .setNA(MIN_INT)
              .setNA(3)
              .setNA(MAX_INT) as DoubleColumn).naValues.max(),
          equals(None));
    });
  });
}

evalColumnSpec() {
  Column mkCol(Iterable<Cell> cells) {
    var cells0 = cells.toList();
    return new Column.eval(
        (row) => (row >= 0 && row < cells0.length) ? cells0(row) : NA);
  }
  baseColumnSpec(mkCol);

  group("eval columns", () {
    test("return dense columns from reindex", () {
      expect(new Column.eval((a) => new Value(a)).reindex([1, 3, 2]),
          new isInstanceOf<DenseColumn>());
    });

    test("return dense columns from force", () {
      expect(new Column.eval((a) => new Value(a)).force(5),
          new isInstanceOf<DenseColumn>());
    });

    test("not overflow index on shift", () {
      expect(new Column.eval((a) => new Value(a)).shift(1).apply(MIN_INT),
          equals(NA));
    });
  });
}

main() {
  columnTest();
}
