library frame.test.column;

import 'dart:math' show Random;
import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

import 'generators.dart' as gen;

//final Random r = new Random();

class ColumnTest<A extends Comparable> {
  final Function rand;

  ColumnTest(A _rand()) : rand = _rand;

  columnTest() {
    List<Cell<A>> slice(Column<A> col, Iterable<int> indices) =>
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
        var col = new Column.dense(int, [1, 2, 3]);
        expect(slice(col, [-1, 0, 1, 2, 3]),
            equals([NA, new Value(1), new Value(2), new Value(3), NA]));
      });

      test("constructable from Cells", () {
        var col = new Column.fromCells([NM, NA, new Value("a"), NM]);
        expect(slice(col, [-1, 0, 1, 2, 3, 4]),
            equals([NA, NM, NA, new Value("a"), NM, NA]));
      });

//      test("wrap row function", () {
//        var col = Column.eval((row) => new Value(-row));
//        expect(col.apply(0), equals(new Value(0)));
//        expect(col.apply(MIN_INT), equals(new Value(MIN_INT)));
//        expect(col.apply(MAX_INT), equals(new Value(-MAX_INT)));
//        expect(col.apply(5), equals(new Value(-5)));
//      });
//
//      test("empty is empty", () {
//        var col = Column.empty /*[String]*/ ();
//        expect(slice(col, [MIN_INT, 0, MAX_INT, -1, 1, 200]), contains(NA));
//      });
    });
  }
/*
    group("memo columns", () {
      test("only evaluate values at most once", () {
        var counter = 0;
        val col = Column.eval((row) {
          counter += 1;
          return Value(row);
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
      var b = new Column<int>();
      var c = new Column<int>([NA, NM, NM]);
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

  baseColumnSpec(Column<A> mkCol(Iterable<Cell<A>> cells)) {
    Gen<Column<A>> genColumn(Gen<A> gen) {
//    for {
//      cellValues <- Gen.listOf(CellGenerators.genCell(gen, (2, 1, 1)))
//    } yield mkCol(cellValues: _*)
    }

    Gen<Mask> genMask() {
//    for {
//      rows0 <- arbitrary[List[Int]]
//      rows = rows0.map(_ & 0xFF)
//    } yield Mask(rows: _*)
    }

//  implicit def arbColumn[A: Arbitrary]: Arbitrary[Column<A>] = Arbitrary(genColumn(arbitrary<A>))

//  implicit val arbMask: Arbitrary[Mask] = Arbitrary(genMask)

    Column<A> column;
    Mask mask;
    setUp(() {
      column = genColumn();
      mask = genMask();
    });

    group("foldRow", () {
      test("return value for Value row", () {
        var col = mkCol(NA, Value(42));
        expect(col.foldRow(1, 0, 0, (a) => 2 + a), equals(44));
      });

      test("return NA value for NA row", () {
        var col = mkCol(NA, NA);
        expect(col.foldRow(1, true, false, (_) => false), isTrue);
      });

      test("return NM value for NM row", () {
        var col = mkCol(NA, NM);
        expect(col.foldRow(1, false, true, (_) => false), isTrue);
      });

      test("fold rows of columns", () {
//      (col: Column[Int], indices: List[Int]) =>
//      var expected = indices.map((i) => col(i)).map {
//        case Value(a) => a.toString
//        case NA => "NA"
//        case NM => "NM"
//      }
        var actual = indices.map((i) => col.foldRow(i, "NA", "NM", i.toString));
        expect(actual, equals(expected));
      });
    });

    group("foreach", () {
      test("bail early on NM values", () {
        var col = mkCol(Value(1), Value(2), NM, Value(4));
        col.foreach(0, 5, (n) => n, (i, n) {
          if (n == 4) {
            throw new Exception();
          }
        });
        ok();
      });

      test("skip NA values", () {
        var col = mkCol(Value(1), NA, Value(2), NA, Value(3));
        var bldr = List.newBuilder();
        col.foreach(0, 5, (n) => 4 - n, (i, n) {
          bldr += n;
        });
        expect(bldr.result(), equals([3, 2, 1]));
      });

      test("work with functions that can't be inlined", () {
        var col = mkCol(Value(1), NA, Value(2), NA, Value(3));
        var bldr = List.newBuilder[Int];
        void f(int a, int b) => (i, n) => bldr += n;
        col.foreach(0, 5, (n) => 4 - n, f);
        expect(bldr.result(), [3, 2, 1]);
      });
    });

    group("map", () {
      test("map each cell in the column", () {
//      (col: Column[Int], indices: List[Int]) =>
        String f(int _) => (n) => (n * 23).toString;
        var col0 = col.map(f);
        expect(indices.map((i) => col(i)).map((c) => c.map(f)),
            equals(indices.map((i) => col0(i))));
      });
    });

    group("flatMap", () {
      test("flatMap each cell in the column", () {
//      (col: Column[Int], indices: List[Int]) =>
        Cell<String> f(int i) => (n) {
              if (n % 3 == 0) {
                return Value((n * 23).toString);
              } else if (n % 3 == 1) {
                return NA;
              } else {
                return NM;
              }
            };
        var col0 = col.flatMap(f);
        expect(indices.map((i) => col(i)).map((c) => c.flatMap(f)),
            equals(indices.map((i) => col0(i))));
      });
    });

    group("reindex", () {
      test("return empty column for empty array", () {
        var col = mkCol(Value(1), Value(2)).reindex(Array());
        range(-10, 11).map((i) => col(i)).forall((c) => expect(c, equals(NA)));
      });

      test("reindex rearranges values", () {
        var col = mkCol(Value(1), Value(2), Value(3));
        expect(col.reindex(Array(1, 2, 0)).slice(range(3)),
            equals([Value(2), Value(3), Value(1)]));
      });

      test("reindex rearranges NMs", () {
        var col = mkCol(Value(1), NM, Value(3), NM);
        expect(col.reindex([1, 2, 3, 0]).slice(range(4)),
            equals([NM, Value(3), NM, Value(1)]));
      });

      test("reindex with negative indices", () {
        var col = mkCol(Value(1), Value(2));
        expect(col.reindex([-1, 0, -1]).slice(range(3)),
            equals([NA, Value(1), NA]));
      });
    });

    group("mask", () {
      test("turn all masked rows into NAs", () {
//      (col: Column[Int], mask: Mask) =>
        var masked = col.mask(mask);
        mask.toSet().forall((i) => expect(masked(i), equals(NA)));
      });

      test("retain unmasked rows as-is", () {
//      (col: Column[Int], rows: List[Int], mask: Mask) =>
        var validRows = rows.toSet(); // -- mask.toSet();
        val masked = col.mask(mask);
        validRows.forall((i) => col(i) == masked(i));
      });
    });

    group("setNA", () {
      test("set the specified row to NA", () {
//      (col: Column[Int], row: Int) =>
        var col0 = col.setNA(row);
        expect(col0(row), equals(NA));
      });

      test("not modify other rows", () {
//      (col: Column[Int], row: Int, rows: List[Int]) =>
        var validRows = rows.toSet() - row;
        var col0 = col.setNA(row);
        validRows.forall((i) => col(i) == col0(i));
      });
    });

    group("force", () {
      test("return empty column when size is 0", () {
//      (col: Column[Int], indices: List[Int]) =>
        var empty = col.force(0);
        indices.map((i) => empty(i)).forall((e) => e == NA);
      });

      test("not mess with values in range", () {
//      (col: Column[Int], blen: Byte) =>
        var len = blen.toInt().abs();
        expect(col.force(len).slice(range(len)), equals(col.slice(range(len))));
      });

      test("NA all values out of range", () {
//      (col: Column[Int], len0: Int, indices: List[Int]) =>
        var len = len0 & 0x7FFFF;
        col
            .force(len)
            .slice(indices.filter((i) => i >= len))
            .forall((v) => v == NA);
      });
    });

    group("shift", () {
      test("shift all rows", () {
//      (col: Column[Int], rows0: Int, indices0: List[Int]) =>
        var rows = rows0 % 100; // Keep it sane for DenseColumns sake.
        var indices = indices0
            .filter((i) => i < Int.MaxValue - 100)
            .filter((i) => i > Int.MinValue + 100);
        var shifted = col.shift(rows);
        expect(indices.map((i) => shifted(i)),
            equals(indices.map((i) => i - rows).map((i) => col(i))));
      });
    });

    group("orElse", () {
      test("be left biased", () {
        var a = mkCol(Value(0), Value(1), Value(2));
        var b = mkCol(Value(0), Value(-1), Value(-2));
        expect(a.orElse(b).apply(1), equals(Value(1)));
        expect(a.orElse(b).apply(2), equals(Value(2)));
        expect(b.orElse(a).apply(1), equals(Value(-1)));
        expect(b.orElse(a).apply(2), equals(Value(-2)));
      });

      test("ignore non values", () {
        var a = mkCol(Value(1), Value(2), NA, NA, NM, NM, NA, NM);
        var b = mkCol(NA, NM, NA, NM, NA, NM, Value(1), Value(2));
        var col = a.orElse(b);

        expect(col.apply(0), equals(Value(1)));
        expect(col.apply(1), equals(Value(2)));
        expect(col.apply(2), equals(NA));
        expect(col.apply(3), equals(NM));
        expect(col.apply(4), equals(NM));
        expect(col.apply(5), equals(NM));
        expect(col.apply(6), equals(Value(1)));
        expect(col.apply(7), equals(Value(2)));
      });
    });

    group("zipMap", () {
      test("promote all NAs with spec type", () {
        var na = mkCol[Int](NA);
        var nm = mkCol[Int](NM);
        var value = mkCol[Int](Value(1));

        expect(na.zipMap(na, (a, b) => a + b, 0), equals(NA));
        expect(na.zipMap(nm, (a, b) => a + b, 0), equals(NA));
        expect(nm.zipMap(na, (a, b) => a + b, 0), equals(NA));
        expect(nm.zipMap(value, (a, b) => a + b, 0), equals(NM));
        expect(value.zipMap(na, (a, b) => a + b, 0), equals(NA));
      });

      test("promote all NAs with unspec type", () {
        var na = mkCol[String](NA);
        var nm = mkCol[String](NM);
        var value = mkCol[String](Value("x"));

        expect(na.zipMap(na, (a, b) => a + b, 0), equals(NA));
        expect(na.zipMap(nm, (a, b) => a + b, 0), equals(NA));
        expect(nm.zipMap(na, (a, b) => a + b, 0), equals(NA));
        expect(nm.zipMap(value, (a, b) => a + b, 0), equals(NM));
        expect(value.zipMap(na, (a, b) => a + b, 0), equals(NA));
      });

      test("NM if both are NM", () {
        var nm0 = mkCol[String](NM);
        var nm1 = mkCol[Int](NM);

        expect(nm0.zipMap(nm1, (a, b) => a + b, 0), equals(NM));
        expect(nm1.zipMap(nm0, (a, b) => a + b, 0), equals(NM));
      });

      test("apply function if both are values", () {
        var col0 = mkCol(Value(1), NA, NM);
        var col1 = mkCol(Value(3.0), Value(2.0), NA);
        var col2 = mkCol(NA, Value("x"), NM);

        expect(col0.zipMap(col0, (a, b) => a + b).slice(range(4)),
            equals([Value(2), NA, NM, NA]));
        expect(col0.zipMap(col1, (a, b) => a + b).slice(range(4)),
            equals([Value(4.0), NA, NA, NA]));
        expect(col0.zipMap(col2, (a, b) => a + b).slice(range(4)),
            equals([NA, NA, NM, NA]));
        expect(col1.zipMap(col0, (a, b) => a + b).slice(range(4)),
            equals([Value(4.0), NA, NA, NA]));
        expect(col1.zipMap(col1, (a, b) => a + b).slice(range(4)),
            equals([Value(6.0), Value(4.0), NA, NA]));
        expect(col1.zipMap(col2, (a, b) => a + b).slice(range(4)),
            equals([NA, Value("2.0x"), NA, NA]));
        expect(col2.zipMap(col0, (a, b) => a + b).slice(range(4)),
            equals([NA, NA, NM, NA]));
        expect(col2.zipMap(col1, (a, b) => a + b).slice(range(4)),
            equals([NA, Value("x2.0"), NA, NA]));
        expect(col2.zipMap(col2, (a, b) => a + b).slice(range(4)),
            equals([NA, Value("xx"), NM, NA]));
      });

      test("conform to same semantics as Cell#zipMap", () {
//      (a: Column[Int], b: Column[Double], indices: List[Int]) =>
        var col = a.zipMap(b, (a, b) => a + b);
        expect(
            indices.map((i) => col(i)),
            equals(
                indices.map((row) => a(row).zipMap(b(row), (a, b) => a + b))));
      });
    });

    group("memoize", () {
      test("calculate values at most once (pessimistic)", () {
//      (col: Column[Int], indices: List[Int]) =>
        var hit = false;
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
//      (col: Column[Int], indices: List[Int]) =>
        var hit = false;
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
    Column<A> mkCol(Iterable<Cell<A>> cells) => new Column.fromCells(cells);
    baseColumnSpec(mkCol);

    group("dense columns", () {
      test("manually spec from dense constructor", () {
        expect(
            Column.dense(["1", "2", "3"]), new isInstanceOf<GenericColumn>());
        expect(Column.dense([1, 2, 3]), new isInstanceOf<IntColumn>());
        expect(Column.dense([1, 2, 3]), new isInstanceOf<LongColumn>());
        expect(Column.dense([1.0, 2.0, 3.0]), new isInstanceOf<DoubleColumn>());
      });

      test("manually spec from default constructor", () {
        expect(Column(NA, Value("x"), NM), new isInstanceOf<GenericColumn>());
        expect(Column(NA, Value(1), NM), new isInstanceOf<IntColumn>());
        expect(Column(NA, Value(1), NM), new isInstanceOf<LongColumn>());
        expect(Column(NA, Value(1.0), NM), new isInstanceOf<DoubleColumn>());
      });

      test("use AnyColumn when type is not known and not-spec", () {
        Column<A> mkCol(Iterable<A> a) => new Column(a.map((aa) => Value(aa)));
        expect(mkCol("x", "y"), new isInstanceOf<AnyColumn>());
      });

      test("use spec col when type is not known but spec", () {
        Column<A> mkCol(Iterable<A> a) =>
            new Column(a.map((aa) => new Value(aa)));
        expect(mkCol(1, 2), new isInstanceOf<IntColumn>());
        expect(mkCol(1, 2), new isInstanceOf<LongColumn>());
        expect(mkCol(1.0, 2.0), new isInstanceOf<DoubleColumn>());
      });

      test("force manual spec through map", () {
        var col = Column.dense(["1", "2", "3"], Mask(1));
        expect(col.map((c) => c.toDouble()), new isInstanceOf<DoubleColumn>());
        expect(col.map((c) => c.toInt()), new isInstanceOf<IntColumn>());
        expect(col.map((c) => c.toLong()), new isInstanceOf<LongColumn>());
      });

      test("force manual spec through flatMap", () {
        var col = Column.dense(["1", "2", "3"], Mask(1));
        expect(col.flatMap((n) => Value(n.toDouble())),
            new isInstanceOf<DoubleColumn>());
        expect(col.flatMap((n) => Value(n.toInt())),
            new isInstanceOf<IntColumn>());
        expect(col.flatMap((n) => Value(n.toLong())),
            new isInstanceOf<LongColumn>());
      });

      test("retain manual spec through filter", () {
        expect(Column.dense([1, 2, 3]).filter((v) => v == 2),
            new isInstanceOf<IntColumn>());
        expect(Column.dense([1.0, 2.0, 3.0]).filter((v) => v == 2),
            new isInstanceOf<DoubleColumn>());
        expect(Column.dense([1, 2, 3]).filter((v) => v == 2),
            new isInstanceOf<LongColumn>());
      });

      test("retain manual spec through orElse", () {
        expect(Column(Value(1)).orElse(Column(Value(2))),
            new isInstanceOf<IntColumn>());
        expect(Column(Value(1)).orElse(Column(Value(2))),
            new isInstanceOf<LongColumn>());
        expect(Column(Value(1.0)).orElse(Column(Value(2.0))),
            new isInstanceOf<DoubleColumn>());
      });

      test("retain manual spec through reindex", () {
        expect(Column.dense([1, 2, 3]).reindex(Array(2, 1, 0)),
            new isInstanceOf<IntColumn>());
        expect(Column.dense([1.0, 2.0, 3.0]).reindex(Array(2, 1, 0)),
            new isInstanceOf<DoubleColumn>());
        expect(Column.dense([1, 2, 3]).reindex(Array(2, 1, 0)),
            new isInstanceOf<LongColumn>());
      });

      test("retain manual spec through force", () {
        expect(Column.dense([1, 2, 3]).force(2), new isInstanceOf<IntColumn>());
        expect(Column.dense([1.0, 2.0, 3.0]).force(2),
            new isInstanceOf<DoubleColumn>());
        expect(
            Column.dense([1, 2, 3]).force(2), new isInstanceOf<LongColumn>());
      });

      test("retain manual spec through mask", () {
        var mask = Mask(1);
        expect(
            Column.dense([1, 2, 3]).mask(mask), new isInstanceOf<IntColumn>());
        expect(Column.dense([1.0, 2.0, 3.0]).mask(mask),
            new isInstanceOf<DoubleColumn>());
        expect(
            Column.dense([1, 2, 3]).mask(mask), new isInstanceOf<LongColumn>());
      });

      test("retain manual spec through setNA", () {
        expect(Column.dense([1, 2, 3]).setNA(Int.MinValue),
            new isInstanceOf<IntColumn>());
        expect(Column.dense([1.0, 2.0, 3.0]).setNA(Int.MinValue),
            new isInstanceOf<DoubleColumn>());
        expect(Column.dense([1, 2, 3]).setNA(Int.MinValue),
            new isInstanceOf<LongColumn>());
      });

      test("setNA should be no-op when row already NA", () {
        var col = Column.dense([1, 2, 3]).asInstanceOf[IntColumn];
        expect(
            Column.dense([1, 2, 3])
                .setNA(Int.MinValue)
                .setNA(3)
                .setNA(Int.MaxValue)
                .asInstanceOf[IntColumn].naValues.max,
            equals(None));
        expect(
            Column.dense([1, 2, 3])
                .setNA(Int.MinValue)
                .setNA(3)
                .setNA(Int.MaxValue)
                .asInstanceOf[LongColumn].naValues.max,
            equals(None));
        expect(
            Column.dense([1.0, 2.0, 3.0])
                .setNA(Int.MinValue)
                .setNA(3)
                .setNA(Int.MaxValue)
                .asInstanceOf[DoubleColumn].naValues.max,
            equals(None));
      });
    });
  }

  evalColumnSpec() {
    Column<A> mkCol(Iterable<Cell<A>> cells) {
      var cells0 = cells.toVector();
      Column.eval((row) => (row >= 0 && row < cells0.size) ? cells0(row) : NA);
    }
    baseColumnSpec(mkCol);

    group("eval columns", () {
      test("return dense columns from reindex", () {
        expect(Column.eval(Value(_)).reindex(Array(1, 3, 2)),
            new isInstanceOf<DenseColumn>());
      });

      test("return dense columns from force", () {
        expect(Column.eval(Value(_)).force(5), new isInstanceOf<DenseColumn>());
      });

      test("not overflow index on shift", () {
        expect(Column.eval(Value(_)).shift(1)(Int.MinValue), equals(NA));
      });
    });
  }
   */
}

//class ColumnOps<A> {
//  Column<A> col;
//  ColumnOps(this.col);
//
//  List<Cell<A>> slice(Iterable<int> rows) =>
//      rows.map((r) => col(r), collection.breakOut);
//}

main() {
  var ct = new ColumnTest<double>(() => gen.r.nextDouble());
  ct.columnTest();
}
