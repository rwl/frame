library frame.test.column;

import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

seriesTest() {
//  rationalMetricSpace() {
//    return new MetricSpace<Rational, Double> {
//      distance(v: Rational, w: Rational) = (v - w).abs.toDouble
//    }

  group("equals should", () {
    test("have a sane equality", () {
      expect(
          new Series(
              [new Tuple2("a", 0), new Tuple2("b", 1), new Tuple2("c", 2)]),
          isNot(equals(new Series(
              [new Tuple2("b", 1), new Tuple2("a", 0), new Tuple2("c", 2)]))));
      expect(new Series([new Tuple2("a", 7)]),
          equals(new Series(Index.fromKeys("a"), Column.dense([7]))));
      expect(
          new Series([new Tuple2("a", 7)]),
          equals(new Series(
              new Index([new Tuple2("a", 0)]), Column.eval((_) => Value(7)))));
      expect(
          new Series([new Tuple2("a", 7)]),
          equals(new Series(
              new Index([new Tuple2("a", 42)]), Column.eval((_) => Value(7)))));
      expect(new Series<String, String>.empty(),
          equals(new Series<String, String>.empty()));
    });
  });

  group("hasValues should", () {
    test("detect values", () {
      Series<String, int> series;
      expect(series.hasValues, equals(series.values.nonEmpty()));
      expect(series.filterByCells((a) => a.isNonValue()).hasValues(), isFalse);
    });
  });

  group("isEmpty should", () {
    test("detect an absence of values", () {
      Series<String, int> series;
      expect(series.isEmpty, isNot(equals(series.values.nonEmpty())));
      expect(series.filterByCells((v) => v.isNonValue()).isEmpty(), isTrue);
    });
  });

  group("++ should", () {
    test("concatenate on series after the other", () {
      var s0 = Series.fromCells([
        new Tuple2(1, Value("a")),
        new Tuple2(1, Value("b")),
        new Tuple2(2, Value("e"))
      ]);
      var s1 = Series
          .fromCells([new Tuple2(0, Value("c")), new Tuple2(1, Value("d"))]);
      expect(
          s0.concat(s1),
          equals(Series.fromCells([
            new Tuple2(1, Value("a")),
            new Tuple2(1, Value("b")),
            new Tuple2(2, Value("e")),
            new Tuple2(0, Value("c")),
            new Tuple2(1, Value("d"))
          ])));
    });

    test("respect original traversal order", () {
      var s0 = Series
          .fromCells([new Tuple2(2, Value("a")), new Tuple2(1, Value("b"))]);
      var s1 = Series
          .fromCells([new Tuple2(1, Value("c")), new Tuple2(0, Value("d"))]);
      expect(
          s0.concat(s1),
          equals(Series.fromCells([
            new Tuple2(2, Value("a")),
            new Tuple2(1, Value("b")),
            new Tuple2(1, Value("c")),
            new Tuple2(0, Value("d"))
          ])));
    });
  });

  group("orElse should", () {
    test("merge series with different number of rows", () {
      var s0 = Series.fromCells([
        new Tuple2(1, Value("a")),
        new Tuple2(1, Value("b")),
        new Tuple2(2, Value("e"))
      ]);
      var s1 = Series
          .fromCells([new Tuple2(0, Value("c")), new Tuple2(1, Value("d"))]);
      expect(
          s0.orElse(s1),
          equals(Series.fromCells([
            new Tuple2(0, Value("c")),
            new Tuple2(1, Value("a")),
            new Tuple2(1, Value("b")),
            new Tuple2(2, Value("e"))
          ])));
    });

    test("always let Value take precedence over all", () {
      var seriesA = Series.fromCells([
        new Tuple2("a", Value("a")),
        new Tuple2("b", NA),
        new Tuple2("c", NM)
      ]);
      var seriesB = Series.fromCells([
        new Tuple2("a", NM),
        new Tuple2("b", Value("b")),
        new Tuple2("c", NA)
      ]);
      var seriesC = Series.fromCells([
        new Tuple2("a", NA),
        new Tuple2("b", NM),
        new Tuple2("c", Value("c"))
      ]);

      var mergedSeries = seriesA.orElse(seriesB).orElse(seriesC);
      expect(mergedSeries("a"), equals(Value("a")));
      expect(mergedSeries("b"), equals(Value("b")));
      expect(mergedSeries("c"), equals(Value("c")));
    });

    test("merge series from left to right and maintain all keys", () {
      var seriesOne = Series.fromCells([
        new Tuple2("a", Value("a1")),
        new Tuple2("b", Value("b1")),
        new Tuple2("c", NM)
      ]);
      var seriesOther = Series.fromCells([
        new Tuple2("b", Value("b2")),
        new Tuple2("c", Value("c2")),
        new Tuple2("x", Value("x2")),
        new Tuple2("y", NA)
      ]);
      var mergedSeriesOne = seriesOne.orElse(seriesOther);
      var mergedSeriesOther = seriesOther.orElse(seriesOne);

      // Ensure all keys are available, grouping by (one |+| other)
      expect(mergedSeriesOne("a"), equals(Value("a1")));
      expect(mergedSeriesOne("b"), equals(Value("b1")));
      expect(mergedSeriesOne("c"), equals(Value("c2")));
      expect(mergedSeriesOne("x"), equals(Value("x2")));
      expect(mergedSeriesOne("y"), equals(NA));

      // Ensure all keys are available, grouping by (other |+| one)
      expect(mergedSeriesOther("a"), equals(Value("a1")));
      expect(mergedSeriesOther("b"), equals(Value("b2")));
      expect(mergedSeriesOther("c"), equals(Value("c2")));
      expect(mergedSeriesOther("x"), equals(Value("x2")));
      expect(mergedSeriesOther("y"), equals(NA));
    });
  });

  group("merge should", () {
    test("merge series with different number of rows", () {
      var s0 = Series.fromCells([
        new Tuple2(1, Value("a")),
        new Tuple2(1, Value("b")),
        new Tuple2(2, Value("e"))
      ]);
      var s1 = Series
          .fromCells([new Tuple2(0, Value("c")), new Tuple2(1, Value("d"))]);
      expect(
          s0.merge(s1),
          equals(Series.fromCells([
            new Tuple2(0, Value("c")),
            new Tuple2(1, Value("ad")),
            new Tuple2(1, Value("b")),
            new Tuple2(2, Value("e"))
          ])));
    });

    test("use Semigroup.op to cogroup values together in merge", () {
      // Contrive a semigroup by adding all the numbers together of the group
//      implicit val intSemigroup = Semigroup.additive[Int]

//      forAll(SeriesGenerators.genSeries(arbitrary[Int], arbitrary[Int], (1, 0, 0))) { series =>
//        var mergedSeries1 = series.merge(series);
//        expect(series.size(), equals(mergedSeries1.size()));
//        series.keys.foreach((key) {
//          expect(mergedSeries1(key).get(), equals(series(key).get() * 2));
//        });
//
//        // Merge once more and ensure it becomes triple the original value
//        var mergedSeries2 = mergedSeries1.merge(series);
//        expect(series.size(), equals(mergedSeries2.size()));
//        series.keys.foreach((key) {
//          expect(mergedSeries2(key).get(), equals(series(key).get() * 3));
//        });
//
//        ok();
//      }
    });

    test("always let NM take precedence over all", () {
      var seriesA = Series.fromCells([
        new Tuple2("a", Value("a")),
        new Tuple2("b", NA),
        new Tuple2("c", NM)
      ]);
      var seriesB = Series.fromCells([
        new Tuple2("a", NM),
        new Tuple2("b", Value("b")),
        new Tuple2("c", NA)
      ]);
      var seriesC = Series.fromCells([
        new Tuple2("a", NA),
        new Tuple2("b", NM),
        new Tuple2("c", Value("c"))
      ]);

      var mergedSeries = seriesA.merge(seriesB).merge(seriesC);
      expect(mergedSeries("a"), equals(NM));
      expect(mergedSeries("b"), equals(NM));
      expect(mergedSeries("c"), equals(NM));
    });

    test("always let Value take precedence over NA", () {
      var seriesA = Series.fromCells([
        new Tuple2("a", Value("a")),
        new Tuple2("b", NA),
        new Tuple2("c", NA)
      ]);
      var seriesB = Series.fromCells([
        new Tuple2("a", NA),
        new Tuple2("b", Value("b")),
        new Tuple2("c", NA)
      ]);
      var seriesC = Series.fromCells([
        new Tuple2("a", NA),
        new Tuple2("b", NA),
        new Tuple2("c", Value("c"))
      ]);

      // Ensure the Value cells simply clobber the NA cells
      var mergedSeries = seriesA.merge(seriesB).merge(seriesC);
      expect(mergedSeries("a"), equals(Value("a")));
      expect(mergedSeries("b"), equals(Value("b")));
      expect(mergedSeries("c"), equals(Value("c")));

      // Ensure further merges of a previously sparse merge operate normally
      var doubleMergedSeries =
          mergedSeries.merge(seriesC).merge(seriesB).merge(seriesA);
      expect(doubleMergedSeries("a"), equals(Value("aa")));
      expect(doubleMergedSeries("b"), equals(Value("bb")));
      expect(doubleMergedSeries("c"), equals(Value("cc")));
    });

    test("merge series from left to right and maintain all keys", () {
      var seriesOne = Series.fromCells([
        new Tuple2("a", Value("a1")),
        new Tuple2("b", Value("b1")),
        new Tuple2("c", NM)
      ]);
      var seriesOther = Series.fromCells([
        new Tuple2("b", Value("b2")),
        new Tuple2("c", Value("c2")),
        new Tuple2("x", Value("x2")),
        new Tuple2("y", NA)
      ]);
      var mergedSeriesOne = seriesOne.merge(seriesOther);
      var mergedSeriesOther = seriesOther.merge(seriesOne);

      // Ensure all keys are available, grouping by (one |+| other)
      expect(mergedSeriesOne("a"), equals(Value("a1")));
      expect(mergedSeriesOne("b"), equals(Value("b1b2")));
      expect(mergedSeriesOne("c"), equals(NM));
      expect(mergedSeriesOne("x"), equals(Value("x2")));
      expect(mergedSeriesOne("y"), equals(NA));

      // Ensure all keys are available, grouping by (other |+| one)
      expect(mergedSeriesOther("a"), equals(Value("a1")));
      expect(mergedSeriesOther("b"), equals(Value("b2b1")));
      expect(mergedSeriesOther("c"), equals(NM));
      expect(mergedSeriesOther("x"), equals(Value("x2")));
      expect(mergedSeriesOther("y"), equals(NA));
    });
  });

  group("map should", () {
    test("map values with original order", () {
      var original =
          Series([new Tuple2("a", 1), new Tuple2("b", 2), new Tuple2("a", 3)]);
      var expected =
          Series([new Tuple2("a", 5), new Tuple2("b", 6), new Tuple2("a", 7)]);
      expect(original.mapValues((v) => v + 4), equals(expected));
    });
  });

  group("cellMap should", () {
    test("transform NAs and NMs", () {
      var original = Series.fromCells(
          [new Tuple2("a", NA), new Tuple2("b", NM), new Tuple2("c", NA)]);
      var expected =
          Series([new Tuple2("a", 1), new Tuple2("b", 2), new Tuple2("c", 1)]);
//      original.cellMap {
//        case NA => Value(1)
//        case NM => Value(2)
//        case Value(_) => Value(3)
//      }, equals(expected
    });

    test("transform Values", () {
      var original =
          Series([new Tuple2("a", 1), new Tuple2("b", 2), new Tuple2("c", 3)]);
      var expected = Series.fromCells([
        new Tuple2("a", NA),
        new Tuple2("b", NM),
        new Tuple2("c", Value(5))
      ]);
//      original.cellMap {
//        case Value(1) => NA
//        case Value(2) => NM
//        case Value(n) => Value(n + 2)
//        case NA | NM => NA
//      }, equals(expected
    });
  });

  group("cellMapWithKeys should", () {
    test("map values with their keys", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            Cell<int> plus5(Cell<int> c) {
//                c match {
//                case Value(v) => Value(v + 5)
//                case NA => NA
//                case NM => NM
            }

            expect(series.cellMapWithKeys((k, v) => plus5(v)),
                equals(series.cellMap(plus5)));
          }
        }
      }
    });
  });

  group("zipMap should", () {
    test("zipMap empty Series", () {
      var empty = new Series<String, String>();
      empty.zipMap(empty, (a, b) => a + b, equals(empty));
    });

    test("zipMap multiple-values-same-key on both sides", () {
      var a = new Series([new Tuple2("a", 1), new Tuple2("a", 2)]);
      var b = new Series([new Tuple2("a", 3), new Tuple2("a", 5)]);
      expect(
          a.zipMap(b, (x, y) => x + y),
          equals(new Series([
            new Tuple2("a", 4),
            new Tuple2("a", 6),
            new Tuple2("a", 5),
            new Tuple2("a", 7)
          ])));
    });

    test("zipMap like an inner join", () {
      var a = new Series([
        new Tuple2("z", 5.0),
        new Tuple2("a", 1.0),
        new Tuple2("b", 3.0),
        new Tuple2("b", 4.0)
      ]);
      var b = new Series(
          [new Tuple2("a", 2), new Tuple2("b", 4), new Tuple2("c", 3)]);

      var c = a.zipMap(b, (x, y) => y * x);
      expect(
          c,
          equals(new Series([
            new Tuple2("a", 2.0),
            new Tuple2("b", 12.0),
            new Tuple2("b", 16.0)
          ])));
    });

    test("let NA cells clobber NM and Value cells", () {
      var one = Series.fromCells([
        new Tuple2("a", Value(1)),
        new Tuple2("b", NA),
        new Tuple2("c", Value(3)),
        new Tuple2("d", NA),
        new Tuple2("d", Value(5)),
        new Tuple2("d", NA),
        new Tuple2("d", Value(7))
      ]);
      var other = Series.fromCells([
        new Tuple2("a", NA),
        new Tuple2("b", Value(2)),
        new Tuple2("c", Value(3)),
        new Tuple2("c", NA),
        new Tuple2("d", Value(5)),
        new Tuple2("d", NM),
        new Tuple2("d", Value(6)),
        new Tuple2("d", Value(7))
      ]);

      expect(one.zipMap(
          other,
          (a, b) => a * b,
          equals(Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", Value(9)),
            new Tuple2("c", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", Value(25)),
            new Tuple2("d", NM),
            new Tuple2("d", Value(30)),
            new Tuple2("d", Value(35)),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", Value(35)),
            new Tuple2("d", NM),
            new Tuple2("d", Value(42)),
            new Tuple2("d", Value(49))
          ]))));
    });

    test("let NM cells clobber NA and Value cells", () {
      var one = Series.fromCells([
        new Tuple2("a", Value(1)),
        new Tuple2("b", NM),
        new Tuple2("c", Value(3)),
        new Tuple2("d", NM),
        new Tuple2("d", Value(5)),
        new Tuple2("d", NA),
        new Tuple2("d", Value(7))
      ]);
      var other = Series.fromCells([
        new Tuple2("a", NM),
        new Tuple2("b", Value(2)),
        new Tuple2("c", Value(3)),
        new Tuple2("c", NM),
        new Tuple2("d", Value(5)),
        new Tuple2("d", NA),
        new Tuple2("d", Value(6)),
        new Tuple2("d", NM)
      ]);

      expect(
          one.zipMap(other, (a, b) => a * b),
          equals(Series.fromCells([
            new Tuple2("a", NM),
            new Tuple2("b", NM),
            new Tuple2("c", Value(9)),
            new Tuple2("c", NM),
            new Tuple2("d", NM),
            new Tuple2("d", NA),
            new Tuple2("d", NM),
            new Tuple2("d", NM),
            new Tuple2("d", Value(25)),
            new Tuple2("d", NA),
            new Tuple2("d", Value(30)),
            new Tuple2("d", NM),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", NA),
            new Tuple2("d", Value(35)),
            new Tuple2("d", NA),
            new Tuple2("d", Value(42)),
            new Tuple2("d", NM)
          ])));
    });
  });

  group("filterByValues should", () {
    test("filter a series by its values", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            expect(series.filterByValues((v) => true).values,
                equals(series.values));
            expect(series.filterByValues((v) => false),
                equals(new Series<String, int>.empty()));
          }
        }
      }
    });
  });

  group("filterByKeys should", () {
    test("filter a series by its keys", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            expect(
                series.filterByKeys((k) => true).values, equals(series.values));
            expect(series.filterByKeys((k) => false),
                equals(new Series<String, int>.empty()));
          }
        }
      }
    });
  });

  group("filterEntries should", () {
    test("filter a series by its key-value pair entries", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            expect(series.filterEntries((k, v) => true), equals(series));
            expect(series.filterEntries((k, v) => false),
                equals(new Series<String, int>.empty()));
          }
        }
      }
    });
  });

  group("foreachDense should", () {
    test("iterate over all dense key-value pairs", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            // Build a sequence of (K, V) from the dense foreach, and ensure the equivalent of the
            // filterByValues dense series can be recreated from its output
            var eachDenseEntry = new Seq.newBuilder<Tuple2<String, int>>();
            series.foreachDense((k, v) => eachDenseEntry += new Tuple2(k, v));
            expect(series.filterByValues((v) => true),
                equals(Series(eachDenseEntry.result() /*: _**/)));
          }
        }
      }
    });
  });

  group("foreachKeys should", () {
    test("iterate over all keys of a series", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            var keyVectorBuilder = new Vector.newBuilder<String>();
            series.foreachKeys((k) => keyVectorBuilder += k);
            expect(series.keys, equals(keyVectorBuilder.result()));
          }
        }
      }
    });
  });

  group("foreachCells should", () {
    test("iterate over all values of a series as cells, including NMs and NAs",
        () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            var cellVectorBuilder = new Vector.newBuilder<Cell<int>>();
            series.foreachCells((c) => cellVectorBuilder += c);
            expect(series.cells, equals(cellVectorBuilder.result()));
          }
        }
      }
    });
  });

  group("foreachValues should", () {
    test("iterate over all the values of a series", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            var valueVectorBuilder = new Vector<int>.newBuilder();
            series.foreachValues((v) => valueVectorBuilder += v);
            expect(series.values, equals(valueVectorBuilder.result()));
          }
        }
      }
    });
  });

  group("sorted should", () {
    test("trivially sort a series", () {
      var a = new Series(
              [new Tuple2("a", 0), new Tuple2("b", 1), new Tuple2("c", 3)])
          .sorted();
      expect(a.sorted(), equals(a));
    });

    test("sort an out-of-order series", () {
      expect(
          new Series(
                  [new Tuple2("c", 0), new Tuple2("a", 1), new Tuple2("b", 3)])
              .sorted(),
          equals(new Series(
              [new Tuple2("a", 1), new Tuple2("b", 3), new Tuple2("c", 0)])));
    });
  });

  group("reduce should", () {
    test("reduce all values", () {
      var a = new Series([
        new Tuple2("a", 1.0),
        new Tuple2("b", 2.0),
        new Tuple2("c", 4.0),
        new Tuple2("d", 5.0)
      ]);
      var b = new Series([
        new Tuple2("c", 1.0),
        new Tuple2("a", 2.0),
        new Tuple2("b", 4.0),
        new Tuple2("a", 5.0)
      ]);
      expect(a.reduce(reduce.Mean), equals(Value(3.0)));
      expect(b.reduce(reduce.Mean), equals(Value(3.0)));
    });

    test("reduce in order", () {
      var a = new Series(
          [new Tuple2("c", 2), new Tuple2("a", 1), new Tuple2("b", 3)]);
      expect(
          a
              .mapValues((v) => v /*:: Nil*/)
              .reduce /*[List[Int]]*/ (reduce.MonoidReducer),
          equals(Value([2, 1, 3])));
    });
  });

  group("reduceByKey should", () {
    test("trivially reduce groups by key", () {
      var a = Series(
          [new Tuple2("a", 1.0), new Tuple2("a", 2.0), new Tuple2("a", 3.0)]);
      expect(a.reduceByKey(reduce.Count), equals(Series([new Tuple2("a", 3)])));
    });

    test("reduce groups by key", () {
      var a = new Series([
        new Tuple2("c", 1.0),
        new Tuple2("a", 2.0),
        new Tuple2("b", 4.0),
        new Tuple2("a", 5.0),
        new Tuple2("b", 2.0),
        new Tuple2("b", 1.0)
      ]);
      var expected = new Series([
        new Tuple2("a", (2.0 + 5.0) / 2),
        new Tuple2("b", (1.0 + 2.0 + 4.0) / 3),
        new Tuple2("c", 1.0)
      ]);
      expect(a.reduceByKey(reduce.Mean), equals(expected));
    });

    test("reduce groups by key in order", () {
      var a = new Series([
        new Tuple2("c", 1),
        new Tuple2("a", 2),
        new Tuple2("b", 3),
        new Tuple2("a", 4),
        new Tuple2("c", 6),
        new Tuple2("c", 5)
      ]);
      var expected = Series([
        new Tuple2("a", [2, 4]),
        new Tuple2("b", [3]),
        new Tuple2("c", [1, 6, 5])
      ]);

      expect(a.mapValues((v) => v /*:: Nil*/).reduceByKey(reduce.MonoidReducer),
          equals(expected));
    });
  });

  Series<K, V> series(Iterable<Tuple2<K, Cell<V>>> kvs) {
    var un = kvs.unzip();
    var keys = un.v1, cells = un.v2;
    return new Series(Index.fromKeys(keys /*: _**/), Column(cells /*: _**/));
  }

  group("rollForward should", () {
    test("roll over NAs", () {
      var s = series([
        new Tuple2(1, Value("a")),
        new Tuple2(2, NA),
        new Tuple2(3, Value("b")),
        new Tuple2(4, NA),
        new Tuple2(5, NA)
      ]);
      expect(
          s.rollForward(),
          equals(series([
            new Tuple2(1, Value("a")),
            new Tuple2(2, Value("a")),
            new Tuple2(3, Value("b")),
            new Tuple2(4, Value("b")),
            new Tuple2(5, Value("b"))
          ])));
    });

    test("skip initial NAs", () {
      val s = series([
        new Tuple2(1, NA),
        new Tuple2(2, NA),
        new Tuple2(3, Value("b")),
        new Tuple2(4, NA),
        new Tuple2(5, NA)
      ]);
      expect(
          s.rollForward(),
          equals(series([
            new Tuple2(1, NA),
            new Tuple2(2, NA),
            new Tuple2(3, Value("b")),
            new Tuple2(4, Value("b")),
            new Tuple2(5, Value("b"))
          ])));
    });

    test("roll NMs forward", () {
      var s0 =
          series([new Tuple2(1, NA), new Tuple2(2, NM), new Tuple2(3, NA)]);
      var s1 = series([
        new Tuple2(1, Value("a")),
        new Tuple2(2, NM),
        new Tuple2(3, NA),
        new Tuple2(4, Value("b"))
      ]);

      expect(
          s0.rollForward(),
          equals(series(
              [new Tuple2(1, NA), new Tuple2(2, NM), new Tuple2(3, NM)])));
      expect(
          s1.rollForward(),
          equals(series([
            new Tuple2(1, Value("a")),
            new Tuple2(2, NM),
            new Tuple2(3, NM),
            new Tuple2(4, Value("b"))
          ])));
    });
  });

  group("rollForwardUpTo should", () {
    test("only roll up to limit", () {
      var s0 = series([
        new Tuple2(1, Value("a")),
        new Tuple2(2, NA),
        new Tuple2(3, NA),
        new Tuple2(4, NA)
      ]);
      var s1 = series([
        new Tuple(0, NA),
        new Tuple2(1, Value("a")),
        new Tuple2(2, NA),
        new Tuple2(3, NA),
        new Tuple2(4, NM),
        new Tuple2(5, Value("b"))
      ]);

      expect(
          s0.rollForwardUpTo(1),
          equals(series([
            new Tuple2(1, Value("a")),
            new Tuple2(2, Value("a")),
            new Tuple2(3, NA),
            new Tuple2(4, NA)
          ])));
      expect(
          s0.rollForwardUpTo(2),
          equals(series([
            new Tuple2(1, Value("a")),
            new Tuple2(2, Value("a")),
            new Tuple2(3, Value("a")),
            new Tuple2(4, NA)
          ])));
      expect(
          s1.rollForwardUpTo(1),
          equals(series([
            new Tuple2(0, NA),
            new Tuple2(1, Value("a")),
            new Tuple2(2, Value("a")),
            new Tuple2(3, NA),
            new Tuple2(4, NM),
            new Tuple2(5, Value("b"))
          ])));
    });

    test("roll NMs", () {
      var s0 = series([
        new Tuple2(1, NM),
        new Tuple2(2, NA),
        new Tuple2(3, NA),
        new Tuple2(4, NA)
      ]);
      expect(
          s0.rollForwardUpTo(1),
          equals(series([
            new Tuple2(1, NM),
            new Tuple2(2, NM),
            new Tuple2(3, NA),
            new Tuple2(4, NA)
          ])));
      expect(
          s0.rollForwardUpTo(2),
          equals(series([
            new Tuple2(1, NM),
            new Tuple2(2, NM),
            new Tuple2(3, NM),
            new Tuple2(4, NA)
          ])));
    });
  });

  group("find(First|Last)Value should", () {
    test("get first/last value in series", () {
      var s0 = new Series([new Tuple2(1, "x"), new Tuple2(2, "y")]);
      expect(s0.findFirstValue(), equals(Some(new Tuple2(1, "x"))));
      expect(s0.findLastValue(), equals(Some(new Tuple2(2, "y"))));

      var s1 = new Series.fromCells([
        new Tuple2(1, NA),
        new Tuple2(2, NM),
        new Tuple2(3, Value("a")),
        new Tuple2(4, NA),
        new Tuple2(5, Value("b")),
        new Tuple2(6, NA)
      ]);
      expect(s1.findFirstValue(), equals(Some(new Tuple2(3, "a"))));
      expect(s1.findLastValue(), equals(Some(new Tuple2(5, "b"))));
    });
  });

  group("closestKeyTo should", () {
    test("always return None for a series with no keys", () {
      var s = new Series<double, double>();
//      forAll (arbitrary[(Double, Double)]) { case (source, tolerance) =>
//        expect(s.closestKeyTo(source, tolerance), equals(None));
//      }
    });

    test("return the value that is closest to the given key", () {
      var s = new Series<double, double>.fromCells([
        new Tuple2(1.0, NM),
        new Tuple2(2.0, NA),
        new Tuple2(3.0, NA),
        new Tuple2(4.0, NM),
        new Tuple2(5.0, NM)
      ]);
      expect(s.closestKeyTo(6, 1), equals(Some(5.0)));
      expect(s.closestKeyTo(6, 0.9), equals(None));
      expect(s.closestKeyTo(4.5, 0.5), equals(Some(4.0)));
      expect(s.closestKeyTo(3.5, 0.4), equals(None));
    });

    test("always return a value within the tolerance, or None", () {
      Series<Rational, Rational> series;
      Rational source;
      double toleranceNegative;
      var tolerance = Math.abs(toleranceNegative);
//        series.closestKeyTo(source, tolerance) match {
//          case Some(value) => {
//            // Ensure the closest key is within the tolerance
//            collect("hit") {
//              expect((source - value).abs().toDouble(), lessThanOrEqualTo(tolerance));
//            }
//          }
//          case None => {
//            // There was no closest, ensure all keys are outside the tolerance
//            collect("miss") {
//              series.keys.forall { key =>
//                expect((key - source).abs().toDouble(), greaterThan(tolerance));
//              }
//            }
//          }
//        }
    });
  });

  group("count should", () {
    test("render the number of items in the series", () {
      Series<String, int> series;
      classifyEmpty(series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            if (series.filterByCells(_ == NM).cells.nonEmpty()) {
              expect(series.count(), equals(NM));
            } else {
              expect(series.count(), equals(Value(series.values.size)));
            }

            ok();
          }
        }
      }
    });
  });

  group("first should", () {
    test("render the first valid value in a series", () {
      expect(new Series<String, int>().first, equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).first,
          equals(Value(1)));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).first,
          equals(Value(2)));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", Value(3))
          ]).first,
          equals(Value(3)));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).first,
          equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", NM)
          ]).first,
          equals(Value(1)));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).first,
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NM)
          ]).first,
          equals(NM));
    });
  });

  group("firstN should", () {
    test("render the first N valid values in a series", () {
      expect(new Series<String, int>().first, equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).firstN(2),
          equals(Value([1, 2])));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).firstN(2),
          equals(Value([2, 3])));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", Value(3))
          ]).firstN(2),
          equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).firstN(2),
          equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NM)
          ]).firstN(2),
          equals(Value([1, 2])));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).firstN(2),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NM)
          ]).firstN(2),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", Value(3))
          ]).firstN(2),
          equals(Value([1, 3])));
    });
  });

  group("last should", () {
    test("render the last valid value in a series", () {
      expect(new Series<String, int>().first, equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).last,
          equals(Value(3)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).last,
          equals(Value(2)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).last,
          equals(Value(1)));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).last,
          equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", NM),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).last,
          equals(Value(3)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", NA)
          ]).last,
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).last,
          equals(NM));
    });
  });

  group("lastN should", () {
    test("render the last N valid values in a series", () {
      expect(new Series<String, int>().lastN(2), equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).lastN(2),
          equals(Value([2, 3])));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).lastN(2),
          equals(Value([1, 2])));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).lastN(2),
          equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).lastN(2),
          equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).lastN(2),
          equals(Value([2, 3])));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).lastN(2),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).lastN(2),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", Value(3))
          ]).lastN(2),
          equals(Value([1, 3])));
    });
  });

  group("max should", () {
    test("render the maximum value in a series", () {
      expect(new Series<String, int>().max(), equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).max(),
          equals(Value(3)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).max(),
          equals(Value(2)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).max(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).max(),
          equals(NA));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).max(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).max(),
          equals(NM));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).max(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", Value(3))
          ]).max(),
          equals(Value(3)));
    });
  });

  group("min should", () {
    test("render the minimum value in a series", () {
      expect(new Series<String, int>().min(), equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).min(),
          equals(Value(1)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).min(),
          equals(Value(1)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).min(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).min(),
          equals(NA));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).min(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).min(),
          equals(NM));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).min(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).min(),
          equals(Value(2)));
    });
  });

  group("sum should", () {
    test("render a sum of all values in a series", () {
      expect(new Series<String, int>().sum(), equals(Value(0)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).sum(),
          equals(Value(6)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).sum(),
          equals(Value(3)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).sum(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).sum(),
          equals(Value(0)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).sum(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).sum(),
          equals(NM));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).sum(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).sum(),
          equals(Value(5)));
    });
  });

  group("sumNonEmpty should", () {
    test("render a sum of all the values in non-empty series", () {
      expect(new Series<String, int>().sumNonEmpty(), equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).sumNonEmpty(),
          equals(Value(6)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).sumNonEmpty(),
          equals(Value(3)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).sumNonEmpty(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).sumNonEmpty(),
          equals(NA));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).sumNonEmpty(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).sumNonEmpty(),
          equals(NM));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).sumNonEmpty(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).sumNonEmpty(),
          equals(Value(5)));
    });
  });

  group("product should", () {
    test("render a product of all values in a series", () {
      expect(new Series<String, int>().product(), equals(Value(1)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).product(),
          equals(Value(6)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).product(),
          equals(Value(2)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).product(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).product(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).product(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).product(),
          equals(NM));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).product(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).product(),
          equals(Value(6)));
    });
  });

  group("productNonEmpty should", () {
    test("render a product of all the values in non-empty series", () {
      expect(new Series<String, int>().productNonEmpty(), equals(NA));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).productNonEmpty(),
          equals(Value(6)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", Value(2)),
            new Tuple2("c", NA)
          ]).productNonEmpty(),
          equals(Value(2)));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).productNonEmpty(),
          equals(Value(1)));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NA),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).productNonEmpty(),
          equals(NA));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).productNonEmpty(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", Value(1)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3))
          ]).productNonEmpty(),
          equals(NM));
      expect(
          Series.fromCells /*[String, Int]*/ ([
            new Tuple2("a", NM),
            new Tuple2("b", NA),
            new Tuple2("c", NA)
          ]).productNonEmpty(),
          equals(NM));
      expect(
          Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2)),
            new Tuple2("c", Value(3))
          ]).productNonEmpty(),
          equals(Value(6)));
    });
  });

  group("histogram should", () {
    test("return 0 counts on empty input series", () {
      expect(
          new Series<String, int>().histogram(0, 10, 2),
          equals(new Series([
            new Tuple2(new Tuple(0, 2), 0),
            new Tuple2(new Tuple(2, 4), 0),
            new Tuple2(new Tuple(4, 6), 0),
            new Tuple2(new Tuple(6, 8), 0),
            new Tuple2(new Tuple2(8, 10), 0)
          ])));
    });

    test("exclude values over max", () {
      expect(
          new Series([
            new Tuple2(1, 1),
            new Tuple2(2, 2),
            new Tuple2(3, 3),
            new Tuple2(4, 4),
            new Tuple2(5, 5)
          ]).histogram(0, 1, 2),
          equals(new Series([new Tuple2(new Tuple2(0, 1), 1)])));

      expect(
          new Series([
            new Tuple2(1, 1),
            new Tuple2(2, 2),
            new Tuple2(3, 3),
            new Tuple2(4, 4),
            new Tuple2(5, 5),
            new Tuple2(6, 6)
          ]).histogram(0, 5, 2),
          equals(new Series([
            new Tuple2(new Tuple(0, 2), 1),
            new Tuple2(new Tuple(2, 4), 2),
            new Tuple(new Tuple(4, 5), 2)
          ])));
    });

    test("max is inclusive", () {
      expect(
          new Series([
            new Tuple2("a", 1),
            new Tuple2("b", 2),
            new Tuple2("c", 3),
            new Tuple2("d", 4),
            new Tuple2("e", 5)
          ]).histogram(0, 4, 2),
          equals(new Series([
            new Tuple2(new Tuple(0, 2), 1),
            new Tuple2(new Tuple(2, 4), 3)
          ])));
    });

    test("exclude values less than min", () {
      expect(
          new Series([
            new Tuple2("a", 1),
            new Tuple2("b", 2),
            new Tuple2("c", 3),
            new Tuple2("d", 4),
            new Tuple2("e", 5)
          ]).histogram(2, 6, 2),
          equals(new Series([
            new Tuple2(new Tuple(2, 4), 2),
            new Tuple2(new Tuple(4, 6), 2)
          ])));

      expect(
          new Series([
            new Tuple2("a", 1),
            new Tuple2("b", 2),
            new Tuple2("c", 3),
            new Tuple2("d", 4),
            new Tuple2("e", 5)
          ]).histogram(3, 6, 2),
          equals(new Series([
            new Tuple2(new Tuple2(3, 5), 2),
            new Tuple2(new Tuple2(5, 6), 1)
          ])));
    });
  });

  group("normalizedHistogram should", () {
//    test("return percentage of cells in bucket", () {
//      expect(new Series((1 to 25).zipWithIndex: _*).normalizedHistogram[Double](0, 24, 5), equals(
//        Series((0, 5) -> 0.2, (5, 10) -> 0.2, (10, 15) -> 0.2, (15, 20) -> 0.2, (20, 24) -> 0.2)));
//    });

    test("include NAs and NMs in total size", () {
      expect(
          Series.fromCells([
            new Tuple2("a", Value(0)),
            new Tuple2("b", NA),
            new Tuple2("c", Value(1)),
            new Tuple2("D", Value(2))
          ]).normalizedHistogram /*[Double]*/ (0, 4, 2),
          equals(new Series([
            new Tuple2(new Tuple(0, 2), 0.5),
            new Tuple2(new Tuple(2, 4), 0.25)
          ])));

      expect(
          Series.fromCells([
            new Tuple2("a", Value(0)),
            new Tuple2("b", Value(3)),
            new Tuple2("c", NM),
            new Tuple2("D", Value(2))
          ]).normalizedHistogram /*[Double]*/ (0, 4, 2),
          equals(Series([
            new Tuple2(new Tuple(0, 2), 0.25),
            new Tuple2(new Tuple(2, 4), 0.5)
          ])));

      expect(
          Series.fromCells([
            new Tuple2("a", Value(0)),
            new Tuple2("b", NA),
            new Tuple2("c", NM),
            new Tuple2("D", Value(2))
          ]).normalizedHistogram /*[Double]*/ (0, 4, 2),
          equals(Series([
            new Tuple2(new Tuple(0, 2), 0.25),
            new Tuple2(new Tuple(2, 4), 0.25)
          ])));
    });
  });
}
