library frame.test.numeric_column_typer;

import 'package:test/test.dart';
import 'package:frame/frame.dart';

/*implicit*/ class ColumnOps<A> {
  ColumnOps(Column<A> col);
  List<Cell<A>> cells(Iterable<int> rows) =>
      rows.map((r) => col(r))(collection.breakOut);
}

numericColumnTyperSpec() {
  var MinInt = Int.MinValue;
  var MaxInt = Int.MaxValue;
  var MinLong = Long.MinValue;
  var MaxLong = Long.MaxValue;

  UntypedColumn untyped(Iterable<A> xs) =>
      TypedColumn(Column.dense(xs.toArray));

  checkCast(Iterable<Tuple2<A, Cell<B>>> casts) {
    val input, output = casts.unzip;
    val col = untyped(input /*: _**/).cast[B]();
    expect(range(0, input.size).map(col(_)), equals(output));
  }

  checkBoundedInteger(A min, A max) {
    var nameA; // = classTag<A>.runtimeClass.getSimpleName

    checkFractional(B fromStr(String s)) {
//      val nameB = classTag<B>.runtimeClass.getSimpleName
      test("small, whole $nameB cast to $nameA is valid", () {
        checkCast([
          new Tuple2(fromStr("100"), Value(new Ring<A>().fromInt(100))),
          new Tuple2(fromStr(min.toString), Value(min))
        ]);
      });
      test("fractional $nameB cast to $nameA is not meaningful", () {
        checkCast(
            [new Tuple2(fromStr("0.1"), NM), new Tuple2(fromStr("100.5"), NM)]);
      });
      test("large $nameB cast to $nameA is not meaningful", () {
        checkCast([
          new Tuple2(2 * fromStr(min.toString), NM),
          new Tuple2(2 * fromStr(max.toString), NM)
        ]);
      });
    }

    test("small BigInt cast to $nameA is valid", () {
      checkCast /*[BigInt, A]*/ ([
        new Tuple2(BigInt(min.toString), Value(min)),
        new Tuple2(BigInt(max.toString), Value(max)),
        new Tuple2(BigInt(0), Value(new Ring<A>.zero())),
        new Tuple2(BigInt(1), Value(new Ring<A>.one()))
      ]);
    });

    test("big BigInt cast to $nameA is not meaningful", () {
      checkCast /*[BigInt, A]*/ ([
        new Tuple2((BigInt(min.toString) - 1), NM),
        new Tuple2((BigInt(max.toString) + 1), NM)
      ]);
    });

    checkFractional[Float]((v) => v.toFloat());
    checkFractional[Double]((v) => v.toDouble());
    checkFractional[BigDecimal]((v) => BigDecimal(v));
    checkFractional[Rational]((v) => Rational(v));
    checkFractional[Number]((v) => Number(v));

    test("cast anything to $nameA", () {
      checkCast /*[Any, A]*/ ([
        new Tuple2(0, Value(new Ring<A>.fromInt(0))),
        new Tuple2(1, Value(new Ring<A>.fromInt(1))),
        new Tuple2(2.0, Value(new Ring<A>.fromInt(2))),
        new Tuple2(3.0, Value(new Ring<A>.fromInt(3))),
        new Tuple2(BigInt(4), Value(new Ring<A>.fromInt(4))),
        new Tuple2(BigDecimal(5), Value(new Ring<A>.fromInt(5))),
        new Tuple2(Rational(6), Value(new Ring<A>.fromInt(6))),
        new Tuple2(Number(7), Value(new Ring<A>.fromInt(7)))
      ]);
    });
  }

  testFractional() {
    String nameA; // = classTag<A>.runtimeClass.getSimpleName
    var convertable = new ConvertableTo<A>();
    //import convertable._

    group("cast anything to $nameA", () {
      checkCast /*[Any, A]*/ ([
        new Tuple2(MaxInt, Value(fromInt(MaxInt))),
        new Tuple2(MinLong, Value(fromLong(MinLong))),
        new Tuple2(BigInt(-100), Value(fromInt(-100))),
        new Tuple2(BigDecimal("0.1"), Value(fromBigDecimal(BigDecimal("0.1")))),
        new Tuple2(Rational(1, 2), Value(fromDouble(0.5))),
        new Tuple2(Number(1.625), Value(fromDouble(1.625)))
      ]);
    });

    test("cast Int to $nameA", () {
      checkCast /*[Int, A]*/ ([
        new Tuple2(MinInt, Value(fromInt(MinInt))),
        new Tuple2(MaxInt, Value(fromInt(MaxInt))),
        new Tuple2(0, Value(fromInt(0))),
        new Tuple2(-10, Value(fromInt(-10)))
      ]);
    });
    test("cast Long to $nameA", () {
      checkCast /*[Long, A]*/ ([
        new Tuple2(MinLong, Value(fromLong(MinLong))),
        new Tuple2(MaxLong, Value(fromLong(MaxLong))),
        new Tuple2(0, Value(fromInt(0))),
        new Tuple2(-10, Value(fromInt(-10)))
      ]);
    });
    test("cast BigInt to $nameA", () {
      checkCast /*[BigInt, A]*/ ([
        new Tuple2(BigInt(0), Value(fromInt(0))),
        new Tuple2(BigInt("-1000000000000"),
            Value(fromBigInt(BigInt("-1000000000000"))))
      ]);
    });
    test("cast Float to $nameA", () {
      checkCast /*[Float, A]*/ ([
        new Tuple2(0.1, Value(fromFloat(0.1))),
        new Tuple2(-1e32, Value(fromFloat(-1e32)))
      ]);
    });
    test("cast Double to $nameA", () {
      checkCast /*[Double, A]*/ ([
        new Tuple2(-0.25, Value(fromDouble(-0.25))),
        new Tuple2(1e100, Value(fromDouble(1e100)))
      ]);
    });
    test("cast BigDecimal to $nameA", () {
      checkCast /*[BigDecimal, A]*/ ([
        new Tuple2(BigDecimal(0), Value(fromInt(0))),
        new Tuple2(BigDecimal("12345.789"),
            Value(fromBigDecimal(BigDecimal("12345.789"))))
      ]);
    });
    test("cast Rational to $nameA", () {
      checkCast /*[Rational, A]*/ ([
        new Tuple2(r"1/3", Value(fromRational(r"1/3"))),
        new Tuple2(r"1234/1235", Value(fromRational(r"1234/1235")))
      ]);
    });

    test("cast Number to $nameA", () {
      checkCast /*[Number, A]*/ ([
        new Tuple2(Number(0), Value(fromInt(0))),
        new Tuple2(Number(BigInt("1000000000001")),
            Value(fromBigInt(BigInt("1000000000001")))),
        new Tuple2(Number(BigDecimal("1.234")),
            Value(fromBigDecimal(BigDecimal("1.234"))))
        //Number(r"1/3") -> Value(fromRational(r"1/3")) // fromRational is broken for Number in Spire.
      ]);
    });
  }

  group("NumericColumnTyper should", () {
    test("cast Int to anything", () {
      var values = [1, 2, MinInt, MaxInt, 0];
      var col = untyped(values);
      var idx = range(5);
      expect(col.cast[Int].cells(idx), equals(values.map((v) => new Value(v))));
      expect(col.cast[Long].cells(idx),
          equals((values.map((n) => new Value(n.toLong)))));
      expect(col.cast[Float].cells(idx),
          equals((values.map((n) => new Value(n.toFloat)))));
      expect(col.cast[Double].cells(idx),
          equals((values.map((n) => new Value(n.toDouble)))));
      expect(col.cast[BigInt].cells(idx),
          equals((values.map((n) => new Value(BigInt(n))))));
      expect(col.cast[BigDecimal].cells(idx),
          equals((values.map((n) => new Value(BigDecimal(n))))));
      expect(col.cast[Rational].cells(idx),
          equals((values.map((n) => new Value(Rational(n))))));
      expect(col.cast[Number].cells(idx),
          equals((values.map((n) => new Value(Number(n))))));
    });

    test("big Long cast to Int is not meaningful", () {
      checkCast /*[Long, Int]*/ ([
        new Tuple2((MaxInt.toLong + 1), NM),
        new Tuple2(MinLong, NM),
        new Tuple2(MaxLong, NM)
      ]);
    });
    test("small Long cast to Int is valid", () {
      checkCast /*[Long, Int]*/ ([
        new Tuple2(MaxInt.toLong, Value(MaxInt)),
        new Tuple2(0, Value(0)),
        new Tuple2(-100, Value(-100))
      ]);
    });
    checkBoundedInteger /*[Int]*/ (MinInt, MaxInt);

    test("Int cast to Long is always valid", () {
      checkCast /*[Int, Long]*/ ([
        new Tuple2(MaxInt, Value(MaxInt.toLong)),
        new Tuple2(0, Value(0)),
        new Tuple2(-100, Value(-100))
      ]);
    });
    checkBoundedInteger /*[Long]*/ (MinLong, MaxLong);

    test("Int cast to BigInt is always valid", () {
      checkCast /*[Int, BigInt]*/ ([
        new Tuple2(MaxInt, Value(BigInt(MaxInt))),
        new Tuple2(0, Value(BigInt(0))),
        new Tuple2(-100, Value(BigInt(-100)))
      ]);
    });

    test("Long cast to BigInt is always valid", () {
      checkCast /*[Long, BigInt]*/ ([
        new Tuple2(MaxLong, Value(BigInt(MaxLong))),
        new Tuple2(0, Value(BigInt(0))),
        new Tuple2(-100, Value(BigInt(-100)))
      ]);
    });

    testFractional[Float]();
    testFractional[Double]();
    testFractional[BigDecimal]();
    testFractional[Rational]();
    testFractional[Number]();
  });
}
