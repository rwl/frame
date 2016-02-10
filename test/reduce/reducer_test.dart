library frame.test.reducer;

import 'package:test/test.dart';
import 'package:frame/frame.dart';

abstract class unique {
  static final dense = new Series.fromPairs([
    new Tuple2("a", 1.0),
    new Tuple2("b", 2.0),
    new Tuple2("c", 4.0),
    new Tuple2("d", 5.0)
  ]);

  static final sparse = new Series(
      new Index.fromKeys("a", "b", "c", "d", "e", "f"),
      new Column.fromValues(NA, Value(2.0), NM, NA, Value(4.0), NM));
}

abstract class odd {
  static final dense = new Series.fromPairs(
      [new Tuple2("a", 1.0), new Tuple2("b", 2.0), new Tuple2("c", 3.0)]);

  static final sparse = Series(Index.fromKeys("a", "b", "c", "d"),
      Column(NA, Value(2.0), Value(4.0), Value(5.0)));
}

abstract class duplicate {
  static final dense = new Series.fromPairs([
    new Tuple2("a", 1.0),
    new Tuple2("a", 2.0),
    new Tuple2("b", 3.0),
    new Tuple2("b", 4.0),
    new Tuple2("b", 5.0),
    new Tuple2("c", 6.0)
  ]);

  static final sparse = new Series.fromPairs([
    new Tuple2("a", NA),
    new Tuple2("b", Value(2.0)),
    new Tuple2("b", NM),
    new Tuple2("b", NA),
    new Tuple2("b", Value(4.0)),
    new Tuple2("b", NM),
    new Tuple2("c", Value(5.0)),
    new Tuple2("c", NA),
    new Tuple2("c", Value(1.0)),
    new Tuple2("d", Value(0.0))
  ]);
}

reducerTest() {
//  implicit val arbRational = Arbitrary(arbitrary[Double].map(Rational(_)))
//  implicit val params = Parameters(minTestsOk = 20, maxDiscardRatio = 20F)

  Prop reducingMeaninglessSeriesMustEqNM /*[I: Arbitrary : ClassTag, O: ClassTag]*/ (
      Reducer<I, O> reducer) {
    forAll(
        arbitrary /*[Series[Int, I]]*/,
        (series) {
          classifyMeaningful(series) {
            expect(series.reduce(reducer) == NM,
                equals(series.cells.contains(NM)));
          }
        }.set(minTestsOk: 10));
  }

  group("Count should", () {
    test("count dense series", () {
      expect(unique.dense.reduce(Count), equals(Value(4)));
      expect(odd.dense.reduce(Count), equals(Value(3)));
      expect(duplicate.dense.reduce(Count), equals(Value(6)));
    });

    test("count sparse series", () {
      expect(odd.sparse.reduce(Count), equals(Value(3)));
    });

    test("count dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Count),
          equals(new Series.fromPairs(
              [new Tuple2("a", 2), new Tuple2("b", 3), new Tuple2("c", 1)])));
    });

    test("count sparse series by key", () {
      test(
          duplicate.sparse.reduceByKey(Count),
          equals(new Series.fromCells([
            new Tuple2("a", Value(0)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(2)),
            new Tuple2("d", Value(1))
          ])));
    });

    test("return the count for series", () {
      /*check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            series.reduce(Count), equals( Value(series.cells.count(_.isValue))
          }
        }
      }*/
    });

    test("return NM if the series contains NM", () {
      reducingMeaninglessSeriesMustEqNM /*[Int, Int]*/ (Count);
    });
  });

  group("First should", () {
    test("get first value in dense series", () {
      expect(unique.dense.reduce(First[Double]), equals(Value(1.0)));
      expect(duplicate.dense.reduce(First[Double]), equals(Value(1.0)));
      expect(odd.dense.reduce(First[Double]), equals(Value(1.0)));

      expect(unique.dense.reduce(FirstN[Double](1)), equals(Value(List(1.0))));
      expect(
          duplicate.dense.reduce(FirstN[Double](1)), equals(Value(List(1.0))));
      expect(odd.dense.reduce(FirstN[Double](1)), equals(Value(List(1.0))));
    });

    test("get first value of sparse series", () {
      expect(unique.sparse.reduce(First[Double]), equals(Value(2.0)));
      expect(duplicate.sparse.reduce(First[Double]), equals(Value(2.0)));
      expect(odd.sparse.reduce(First[Double]), equals(Value(2.0)));

      expect(unique.sparse.reduce(FirstN[Double](1)), equals(Value(List(2.0))));
      expect(
          duplicate.sparse.reduce(FirstN[Double](1)), equals(Value(List(2.0))));
      expect(odd.sparse.reduce(FirstN[Double](1)), equals(Value(List(2.0))));
    });

    test("get first in dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(First[Double]),
          equals(new Series.fromPairs([
            new Tuple2("a", 1.0),
            new Tuple2("b", 3.0),
            new Tuple2("c", 6.0)
          ])));

      expect(
          duplicate.dense.reduceByKey(FirstN[Double](1)),
          equals(new Series.fromPairs([
            new Tuple2("a", List(1.0)),
            new Tuple2("b", List(3.0)),
            new Tuple2("c", List(6.0))
          ])));
    });

    test("get first in sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(First[Double]),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(2.0)),
            new Tuple2("c", Value(5.0)),
            new Tuple2("d", Value(0.0))
          ])));

      expect(
          duplicate.sparse.reduceByKey(FirstN[Double](1)),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(List(2.0))),
            new Tuple2("c", Value(List(5.0))),
            new Tuple2("d", Value(List(0.0)))
          ])));
    });

    test("return the first value in a series", () {
      forAll(arbitrary /*[Series[Int, Int]]*/, (series) {
        classifySparse(series) {
          classifyMeaningful(series) {
            var reduction = series.reduce(First[Int]);
            if (!series.cells.exists((c) => c != NA)) {
              expect(reduction, equals(NA));
            } else {
              expect(
                  reduction, equals(series.cells.filter((c) => c != NA).head));
            }
          }
        }
      });
    });
  });

  group("FirstN should", () {
    test("get first N values in dense series", () {
      expect(unique.dense.reduce(FirstN[Double](3)),
          equals(Value(List(1.0, 2.0, 4.0))));
      expect(duplicate.dense.reduce(FirstN[Double](3)),
          equals(Value(List(1.0, 2.0, 3.0))));
      expect(odd.dense.reduce(FirstN[Double](3)),
          equals(Value(List(1.0, 2.0, 3.0))));
    });

    test("get first N values in sparse series", () {
      expect(odd.sparse.reduce(FirstN[Double](3)),
          equals(Value(List(2.0, 4.0, 5.0))));
    });

    test("get first N values in dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(FirstN[Double](2)),
          equals(new Series.fromCells([
            new Tuple2("a", Value(List(1.0, 2.0))),
            new Tuple2("b", Value(List(3.0, 4.0))),
            new Tuple2("c", NA)
          ])));

      expect(
          duplicate.dense.reduceByKey(FirstN[Double](3)),
          equals(Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(List(3.0, 4.0, 5.0))),
            new Tuple2("c", NA)
          ])));
    });

    test("get first N values in sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(FirstN[Double](2)),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(List(5.0, 1.0))),
            new Tuple2("d", NA)
          ])));
    });

    test("return the first n values in a series", () {
      forAll(arbitrary /*[Series[Int, Int]])*/, (series) {
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              forAll(Gen.choose(1, series.size), (n) {
                var takeN = series.cells.filter((c) => c != NA).take(n);
                if (takeN.contains(NM)) {
                  // If the firstN contains an NM, the result must be NM
                  expect(series.reduce(FirstN[Int](n)), equals(NM));
                } else if (takeN.length < n) {
                  // If there are not enough valid values, the result must be NA
                  expect(series.reduce(FirstN[Int](n)), equals(NA));
                } else {
                  // Otherwise, we should have a valid range of only the valid Values
                  expect(series.reduce(FirstN[Int](n)),
                      equals(Value(takeN.map((v) => v.get))));
                }
              });
            }
          }
        }
      });
    });
  });

  group("Last should", () {
    test("get last value in dense series", () {
      expect(unique.dense.reduce(Last[Double]), equals(Value(5.0)));
      expect(duplicate.dense.reduce(Last[Double]), equals(Value(6.0)));
      expect(odd.dense.reduce(Last[Double]), equals(Value(3.0)));

      expect(unique.dense.reduce(LastN[Double](1)), equals(Value(List(5.0))));
      expect(
          duplicate.dense.reduce(LastN[Double](1)), equals(Value(List(6.0))));
      expect(odd.dense.reduce(LastN[Double](1)), equals(Value(List(3.0))));
    });

    test("get last value of sparse series", () {
      expect(duplicate.sparse.reduce(Last[Double]), equals(Value(0.0)));
      expect(odd.sparse.reduce(Last[Double]), equals(Value(5.0)));

      expect(
          duplicate.sparse.reduce(LastN[Double](1)), equals(Value(List(0.0))));
      expect(odd.sparse.reduce(LastN[Double](1)), equals(Value(List(5.0))));
    });

    test("get last in dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Last[Double]),
          equals(new Series([
            new Tuple2("a", 2.0),
            new Tuple2("b", 5.0),
            new Tuple2("c", 6.0)
          ])));

      expect(
          duplicate.dense.reduceByKey(LastN[Double](1)),
          equals(new Series([
            new Tuple2("a", List(2.0)),
            new Tuple2("b", List(5.0)),
            new Tuple2("c", List(6.0))
          ])));
    });

    test("get last in sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(Last[Double]),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(1.0)),
            new Tuple2("d", Value(0.0))
          ])));

      expect(
          duplicate.sparse.reduceByKey(LastN[Double](1)),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(List(1.0))),
            new Tuple2("d", Value(List(0.0)))
          ])));
    });

    test("return the last value in a series", () {
      forAll(arbitrary /*[Series[Int, Int]]*/, (series) {
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              var reduction = series.reduce(Last[Int]);
              if (!series.cells.exists((c) => c != NA)) {
                expect(reduction, equals(NA));
              } else {
                expect(reduction,
                    equals(series.cells.filter((c) => c != NA).last));
              }
            }
          }
        }
      });
    });
  });

  group("LastN should", () {
    test("get last N values in dense series", () {
      expect(unique.dense.reduce(LastN[Double](3)),
          equals(Value(List(2.0, 4.0, 5.0))));
      expect(duplicate.dense.reduce(LastN[Double](3)),
          equals(Value(List(4.0, 5.0, 6.0))));
      expect(odd.dense.reduce(LastN[Double](3)),
          equals(Value(List(1.0, 2.0, 3.0))));
    });

    test("get last N values in sparse series", () {
      expect(odd.sparse.reduce(LastN[Double](3)),
          equals(Value(List(2.0, 4.0, 5.0))));
    });

    test("get last N values in dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(LastN[Double](2)),
          equals(new Series.fromCells([
            new Tuple2("a", Value(List(1.0, 2.0))),
            new Tuple2("b", Value(List(4.0, 5.0))),
            new Tuple2("c", NA)
          ])));

      expect(
          duplicate.dense.reduceByKey(LastN[Double](3)),
          equals(Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", Value(List(3.0, 4.0, 5.0))),
            new Tuple2("c", NA)
          ])));
    });

    test("get last N values in sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(LastN[Double](2)),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(List(5.0, 1.0))),
            new Tuple2("d", NA)
          ])));
    });

    test("return the last n values in a series", () {
      forAll(arbitrary /*[Series[Int, Int]])*/, (series) {
        classifyEmpty(series) {
          classifySparse(series) {
            classifyMeaningful(series) {
              forAll(Gen.choose(1, series.size), (n) {
                var reduction = series.reduce(LastN[Int](n));
                var takeN = series.cells.filter(_ != NA).takeRight(n);
                if (takeN.contains(NM)) {
                  // If the lastN contains an NM, the result must be NM
                  expect(reduction, equals(NM));
                } else if (takeN.length < n) {
                  // If there is no valid Value, the result must be NA
                  expect(reduction, equals(NA));
                } else {
                  // Otherwise, we should have a valid range of only the valid Values
                  expect(reduction, equals(Value(takeN.map((v) => v.get))));
                }
              });
            }
          }
        }
      });
    });
  });

  group("Max should", () {
    test("find max in dense series", () {
      expect(unique.dense.reduce(Max[Double]), equals(Value(5.0)));
      expect(odd.dense.reduce(Max[Double]), equals(Value(3.0)));
      expect(duplicate.dense.reduce(Max[Double]), equals(Value(6.0)));
    });

    test("find max in sparse series", () {
      expect(odd.sparse.reduce(Max[Double]), equals(Value(5.0)));
    });

    test("find max in dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Max[Double]),
          equals(Series([
            new Tuple2("a", 2.0),
            new Tuple2("b", 5.0),
            new Tuple2("c", 6.0)
          ])));
    });

    test("find max in sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(Max[Double]),
          equals(Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(5.0)),
            new Tuple2("d", Value(0.0))
          ])));
    });

    test("return the max value of a series", () {
      /*check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              series.reduce(Max[Int]), equals( NA
            } else {
              series.reduce(Max[Int]), equals( Value(series.values.max)
            }
          }
        }
      }*/
    });

    test("return NM if the series contains NM", () {
      reducingMeaninglessSeriesMustEqNM(Max[Int]);
    });
  });

  group("Mean should", () {
    test("find mean of dense series", () {
      expect(unique.dense.reduce(Mean[Double]), equals(Value(3.0)));
      expect(duplicate.dense.reduce(Mean[Double]), equals(Value(3.5)));
    });

    test("find mean of sparse series", () {
      expect(odd.sparse.reduce(Mean[Double]), equals(Value(11.0 / 3.0)));
    });

    test("find mean of dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Mean[Double]),
          equals(Series([
            new Tuple2("a", 1.5),
            new Tuple2("b", 4.0),
            new Tuple2("c", 6.0)
          ])));
    });

    test("find mean of sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(Mean[Double]),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3.0)),
            new Tuple2("d", Value(0.0))
          ])));
    });

    test("return the mean value of a series", () {
      /*check1[MeaningfulSeries[Int, Rational], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              series.reduce(Mean[Rational]), equals( NA
            } else {
              val mean = series.values.qsum / series.values.length
              series.reduce(Mean[Rational]), equals( Value(mean)
            }
          }
        }
      }*/
    });

    test("return NM if the series contains NM", () {
      reducingMeaninglessSeriesMustEqNM(Mean[Double]);
    });
  });

  group("Median should", () {
    test("find median in dense series", () {
      expect(unique.dense.reduce(Median[Double]), equals(Value(3.0)));
      expect(duplicate.dense.reduce(Median[Double]), equals(Value(3.5)));
      expect(odd.dense.reduce(Median[Double]), equals(Value(2.0)));
    });

    test("find median in sparse series", () {
      expect(odd.sparse.reduce(Median[Double]), equals(Value(4.0)));
    });

    test("find median in dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Median[Double]),
          equals(Series([
            new Tuple2("a", 1.5),
            new Tuple2("b", 4.0),
            new Tuple2("c", 6.0)
          ])));
    });

    test("find median in sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(Median[Double]),
          equals(new Series.fromCells([
            new Tuple2("a", NA),
            new Tuple2("b", NM),
            new Tuple2("c", Value(3.0)),
            new Tuple2("d", Value(0.0))
          ])));
    });

    test("return the median value of a series", () {
      /*check1[MeaningfulSeries[Int, Double], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            val dense = series.values.sorted
            if (dense.isEmpty) {
              series.reduce(Median[Double]), equals( NA
            } else {
              val dense = series.values.sorted
              val l = (dense.size - 1) / 2
              val u = dense.size / 2
              val median = (dense(l) + dense(u)) / 2
              series.reduce(Median[Double]), equals( Value(median)
            }
          }
        }
      }*/
    });

    test("return NM if the series contains NM", () {
      reducingMeaninglessSeriesMustEqNM(Median[Double]);
    });
  });

  group("Monoid should", () {
    test("sum dense series with additive monoid", () {
      expect(unique.dense.reduce(Sum[Double]), equals(Value(12.0)));
      expect(duplicate.dense.reduce(Sum[Double]), equals(Value(21.0)));
    });

    test("sum sparse series with additive monoid", () {
      expect(odd.sparse.reduce(Sum[Double]), equals(Value(11.0)));
    });

    test("sum dense series by key with additive monoid", () {
      expect(
          duplicate.dense.reduceByKey(Sum[Double]),
          equals(Series([
            new Tuple2("a", 3.0),
            new Tuple2("b", 12.0),
            new Tuple2("c", 6.0)
          ])));
    });

    test("sum sparse series by key with additive monoid", () {
      expect(
          duplicate.sparse.reduceByKey(Sum[Double]),
          equals(Series([
            new Tuple2("a", 0.0),
            new Tuple2("b", NM),
            new Tuple2("c", 6.0),
            new Tuple2("d", 0.0)
          ])));
    });

    test("return the monoidal reduction for a series", () {
      /*check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              // For empty series, ensure the reducers return the identity value
              {
                implicit val m = Monoid.additive[Int]
                expect(series.reduce(MonoidReducer[Int]), equals( Value(m.id)));
              } and {
                implicit val m = Monoid.multiplicative[Int]
                expect(series.reduce(MonoidReducer[Int]), equals( Value(m.id)));
              }
            } else {
              // For non-empty series, ensure the reducers return the correct value
              {
                implicit val m = Monoid.additive[Int]
                expect(series.reduce(MonoidReducer[Int]), equals( Value(series.values.sum)));
              } and {
                implicit val m = Monoid.multiplicative[Int]
                expect(series.reduce(MonoidReducer[Int]), equals( Value(series.values.product)));
              }
            }
          }
        }
      }*/
    });

    test("return NM if the series contains NM", () {
      //implicit val m = Monoid.additive[Int]
      reducingMeaninglessSeriesMustEqNM(MonoidReducer[Int]);
    });
  });

  group("SemiGroup should", () {
    test("reduce by key", () {
      var s = Series.fromCells([
        new Tuple2(1, Value("a")),
        new Tuple2(1, Value("b")),
        new Tuple2(2, NA),
        new Tuple2(2, Value("c")),
        new Tuple2(3, Value("d")),
        new Tuple2(3, NA),
        new Tuple2(3, Value("e")),
        new Tuple2(3, NA),
        new Tuple2(3, Value("e")),
        new Tuple2(4, NA),
        new Tuple2(4, NA)
      ]);
      expect(
          s.reduceByKey(SemigroupReducer[String]),
          equals(Series.fromCells([
            new Tuple2(1, Value("ab")),
            new Tuple2(2, Value("c")),
            new Tuple2(3, Value("dee")),
            new Tuple2(4, NA)
          ])));
    });

    test("return the semi-group reduction of a series", () {
      /*check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              // For empty series, ensure the reducers return NA
              {
                implicit val g = Semigroup.additive[Int]
                expect(series.reduce(SemigroupReducer[Int]), equals( NA));
              } and {
                implicit val g = Semigroup.multiplicative[Int]
                expect(series.reduce(SemigroupReducer[Int]), equals( NA));
              }
            } else {
              // For non-empty series, ensure the reducers return the correct value
              {
                implicit val m = Semigroup.additive[Int]
                expect(series.reduce(SemigroupReducer[Int]), equals( Value(series.values.sum)));
              } and {
                implicit val m = Semigroup.multiplicative[Int]
                expect(series.reduce(SemigroupReducer[Int]), equals( Value(series.values.product)));
              }
            }
          }
        }
      }*/
    });

    //implicit val m = Semigroup.additive[Int]
    test("return NM if the series contains NM", () {
      reducingMeaninglessSeriesMustEqNM(SemigroupReducer[Int]);
    });
  });

  group("Unique should", () {
    test("return unique elements from dense series", () {
      expect(unique.dense.reduce(Unique[Double]),
          equals(Value(Set(1.0, 2.0, 4.0, 5.0))));
      expect(
          odd.dense.reduce(Unique[Double]), equals(Value(Set(1.0, 2.0, 3.0))));
      expect(duplicate.dense.reduce(Unique[Double]),
          equals(Value(Set(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))));

      expect(
          new Series([
            new Tuple2(1, 1),
            new Tuple2(2, 1),
            new Tuple2(3, 2),
            new Tuple2(4, 1),
            new Tuple2(5, 3),
            new Tuple2(6, 2)
          ]).reduce(Unique[Int]),
          equals(Value(Set(1, 2, 3))));
    });

    test("return unique elements in sparse series", () {
      expect(
          odd.sparse.reduce(Unique[Double]), equals(Value(Set(2.0, 4.0, 5.0))));

      var s = new Series.fromCells([
        new Tuple2(1, Value("a")),
        new Tuple2(2, NA),
        new Tuple2(1, Value("b")),
        new Tuple2(3, NA)
      ]);
      expect(s.reduce(Unique[String]), equals(Value(Set("a", "b"))));
    });

    test("return unique elements from dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Unique[Double]),
          equals(Series([
            new Tuple2("a", Set(1.0, 2.0)),
            new Tuple2("b", Set(3.0, 4.0, 5.0)),
            new Tuple2("c", Set(6.0))
          ])));
    });

    test("return unique elements from sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(Unique[Double]),
          equals(new Series.fromCells([
            new Tuple2("a", Value(Set.empty)),
            new Tuple2("b", NM),
            new Tuple2("c", Value(Set(1.0, 5.0))),
            new Tuple2("d", Value(Set(0.0)))
          ])));

      var s = Series.fromCells([
        new Tuple2(1, Value("a")),
        new Tuple2(1, Value("b")),
        new Tuple2(2, NA),
        new Tuple2(2, Value("c")),
        new Tuple2(3, Value("d")),
        new Tuple2(3, NA),
        new Tuple2(3, Value("e")),
        new Tuple2(3, NA),
        new Tuple2(3, Value("e")),
        new Tuple2(4, NA),
        new Tuple2(4, NA)
      ]);
      expect(
          s.reduceByKey(Unique[String]),
          equals(Series([
            new Tuple2(1, Set("a", "b")),
            new Tuple2(2, Set("c")),
            new Tuple2(3, Set("d", "e")),
            new Tuple2(4, Set.empty)
          ])));
    });

    test("return the unique values for a series", () {
      /*check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              expect(series.reduce(Unique[Int]), equals( Value(Set.empty)));
            } else {
              expect(series.reduce(Unique[Int]), equals( Value(series.values.toSet)));
            }
          }
        }
      }*/
    });

    test("return NM if the series contains NM", () {
      reducingMeaninglessSeriesMustEqNM(Unique[Int]);
    });
  });

  group("Exists should", () {
    bool pAll(int i) => true;
    bool pNone(int i) => false;
    bool pMod10(int i) => i % 10 == 0;

    test("existentially quantify predicate over a dense series", () {
      expect(unique.dense.reduce(Exists[Double]((_) => true)),
          equals(Value(true)));
      expect(unique.dense.reduce(Exists[Double]((_) => false)),
          equals(Value(false)));
      expect(unique.dense.reduce(Exists[Double]((d) => d < 2.0)),
          equals(Value(true)));
    });

    test("existentially quantify predicate over sparse series", () {
      expect(unique.sparse.reduce(Exists[Double]((d) => d < 3.0)),
          equals(Value(true)));
      expect(odd.sparse.reduce(Exists[Double]((d) => d < 3.0)),
          equals(Value(true)));
      expect(duplicate.sparse.reduce(Exists[Double]((d) => d < 3.0)),
          equals(Value(true)));
    });

    test("existentially quantify predicate over a dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(Exists[Double]((d) => d < 2.0)),
          equals(Series([
            new Tuple2("a", true),
            new Tuple2("b", false),
            new Tuple2("c", false)
          ])));
    });

    test("existentially quantify predicate over sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(Exists[Double]((d) => d < 2.0)),
          equals(Series([
            new Tuple2("a", false),
            new Tuple2("b", false),
            new Tuple2("c", true),
            new Tuple2("d", true)
          ])));
    });

    test("return whether or not a predicate exists for a series", () {
      /*check1[MeaningfulSeries[Int, Int], Prop] { case MeaningfulSeries(series) =>
        classifyEmpty(series) {
          classifySparse(series) {
            if (series.values.isEmpty) {
              expect(series.reduce(Exists[Int](pAll)), equals( Value(false)));
            } else {
              classify(series.values.exists(pMod10), "exists=true", "exists=false") {
                expect(series.reduce(Exists(pNone)), equals( Value(false)));
                expect(series.reduce(Exists(pMod10)), equals( Value(series.values.exists(pMod10))));
              }
            }
          }
        }
      }*/
    });
  });

  group("Quantile should", () {
    test("return NA for empty series", () {
      /*check1[EmptySeries[Int, Rational], Prop] { case EmptySeries(series) =>
        collect(series.size) {
          expect(series.reduce(Quantile[Rational](List(0.25, 0.5, 0.75))), equals( NA));
        }
      }.set(minTestsOk = 10)*/
    });

    test("return min value for 0p", () {
      /*forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          val min = series.values.min
          expect(series.reduce(Quantile[Rational](List(0.0))), equals( Value(List(new Tuple2(0.0, min)))));
        }
      }*/
    });

    test("return max value for 1p", () {
      /*forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          val max = series.values.max
          expect(series.reduce(Quantile[Rational](List(1.0))), equals( Value(List(new Tuple2(1.0, max)))));
        }
      }*/
    });

    test("never return percentile below min or above max", () {
      /*forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
          classifySparse(series) {
            var min = series.values.min;
            var max = series.values.max;
            series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (_, q) =>
              expect(q, greaterThanOrEqualTo(min));
              expect(q, lessThanOrEqualTo(max));
            }
          }
        }
      }*/
    });

    test("percentiles split at appropriate mark", () {
      /*forAll(arbitrary[MeaningfulSeries[Int, Rational]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        forAll(Gen.listOf(Gen.choose(0d, 1d))) { quantiles =>
          series.reduce(Quantile[Rational](quantiles)).value.get.forall { case (p, q) =>
            var below = math.ceil(series.values.size * p)
            var above = math.ceil(series.values.size * (1 - p))
            series.values.count(_ < q) <= below && series.values.count(_ > q) <= above
          }
        }
      }*/
    });
  });

  group("ForAll should", () {
    bool pTrue(int _) => true;
    bool pFalse(int _) => false;
    bool pPositive(int i) => i > 0;

    test("universally quantify predicate over a dense series", () {
      expect(unique.dense.reduce(ForAll[Double]((_) => true)),
          equals(Value(true)));
      expect(unique.dense.reduce(ForAll[Double]((_) => false)),
          equals(Value(false)));
      expect(unique.dense.reduce(ForAll[Double]((d) => d > 0.0)),
          equals(Value(true)));
    });

    test(
        "universally quantify predicate over a sparse series with only unavailablity",
        () {
      expect(odd.sparse.reduce(ForAll[Double]((d) => d > 0.0)),
          equals(Value(true)));
    });

    test(
        "universally quantify predicate over a sparse series with not meaningful values",
        () {
      expect(unique.sparse.reduce(ForAll[Double]((_) => true)),
          equals(Value(false)));
      expect(unique.sparse.reduce(ForAll[Double]((_) => false)),
          equals(Value(false)));
      expect(duplicate.sparse.reduce(ForAll[Double]((_) => true)),
          equals(Value(false)));
      expect(duplicate.sparse.reduce(ForAll[Double]((_) => false)),
          equals(Value(false)));
    });

    test("universally quantify predicate over a dense series by key", () {
      expect(
          duplicate.dense.reduceByKey(ForAll[Double]((_) => false)),
          equals(Series([
            new Tuple2("a", false),
            new Tuple2("b", false),
            new Tuple2("c", false)
          ])));
      expect(
          duplicate.dense.reduceByKey(ForAll[Double]((_) => true)),
          equals(Series([
            new Tuple2("a", true),
            new Tuple2("b", true),
            new Tuple2("c", true)
          ])));
      expect(
          duplicate.dense.reduceByKey(ForAll[Double]((d) => d < 6.0)),
          equals(Series([
            new Tuple2("a", true),
            new Tuple2("b", true),
            new Tuple2("c", false)
          ])));
    });

    test("universally quantify predicate over a sparse series by key", () {
      expect(
          duplicate.sparse.reduceByKey(ForAll[Double]((_) => false)),
          equals(Series([
            new Tuple2("a", true),
            new Tuple2("b", false),
            new Tuple2("c", false),
            new Tuple2("d", false)
          ])));
      expect(
          duplicate.sparse.reduceByKey(ForAll[Double]((_) => true)),
          equals(Series([
            new Tuple2("a", true),
            new Tuple2("b", false),
            new Tuple2("c", true),
            new Tuple2("d", true)
          ])));
      expect(
          duplicate.sparse.reduceByKey(ForAll[Double]((d) => d < 5.0)),
          equals(Series([
            new Tuple2("a", true),
            new Tuple2("b", false),
            new Tuple2("c", false),
            new Tuple2("d", true)
          ])));
    });

    test("return true for an empty series", () {
      /*check1[EmptySeries[Int, Int], Prop] { case EmptySeries(series) =>
        collect(series.size) {
          expect(series.reduce(ForAll[Int](pFalse)), equals( Value(true)));
        }
      }.set(minTestsOk = 10)*/
    });

    test("return false for a series that contains NM", () {
      /*forAll(arbitrary[Series[Int, Int]]) { series =>
        classifyMeaningful(series) {
          expect(series.reduce(ForAll[Int](pTrue)), equals( Value(!series.cells.contains(NM))));
        }
      }*/
    });

    test("evaluate the predicate for a series", () {
      /*forAll(arbitrary[MeaningfulSeries[Int, Int]].suchThat(_.series.values.nonEmpty)) { case MeaningfulSeries(series) =>
        classifySparse(series) {
          classify(series.values.min > 0, "forall=true", "forall=false") {
            expect(series.reduce(ForAll[Int](pPositive)), equals( Value(series.values.min > 0)));
          }
        }
      }*/
    });
  });
}
