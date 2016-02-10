library frame.test.stats;

import 'package:test/test.dart';
import 'package:frame/frame.dart';

summaryTest() {
  var stats = List(summary.Mean, summary.Median, summary.Max, summary.Min);
  var statsIndex = Index.fromKeys(stats);

  group("summary from Frame should", () {
    test("be empty for empty frame", () {
      expect(summary(new Frame.empty<int, int>()),
          equals(new Frame.empty<int, int>.withColIndex(statsIndex)));
    });

    test("be NA for empty col", () {
      var emptyFrame = new Series<int, double>.fromCells(
              [new Tuple2(0, NA), new Tuple2(1, NA), new Tuple2(2, NA)])
          .toFrame("x");
      var expected =
          new Frame<String, String, double>.fill(["x"], stats, (_, __) => NA);
      expect(summary(emptyFrame), equals(expected));
    });

    test("summarize dense frame", () {
      var input = Frame.fromRows([
        [1, 2, HNil],
        [3, 3, HNil],
        [2, 4, HNil]
      ]);

      var expected = Frame.fromRows([
        [2, 2, 3, 1, HNil],
        [3, 3, 4, 2, HNil]
      ]).withColIndex(statsIndex);

      expect(input.summary, equals(expected));
    });

    test("summarize sparse frame", () {
      var input = Frame.mergeColumns([
        new Tuple2(
            0,
            new Series<int, int>.fromCells([
              new Tuple2(0, NA),
              new Tuple2(1, Value(3)),
              new Tuple2(2, Value(2)),
              new Tuple2(3, NA),
              new Tuple2(4, Value(1))
            ])),
        new Tuple2(
            1,
            new Series<int, int>.fromCells([
              new Tuple2(0, Value(2)),
              new Tuple2(1, NM),
              new Tuple2(2, NM),
              new Tuple2(3, Value(4)),
              new Tuple2(4, Value(3))
            ]))
      ]);

      var expected = new Frame.fill([0, 1], stats, (i, stat) {
        if (i == 0 && stat == summary.Mean) {
          return new Value(2);
        } else if (i == 0 && stat == summary.Median) {
          return new Value(2);
        } else if (i == 0 && stat == summary.Max) {
          return new Value(3);
        } else if (i == 0 && stat == summary.Min) {
          return new Value(1);
        } else if (i == 1 && stat == summary.Mean) {
          return NM;
        } else if (i == 1 && stat == summary.Median) {
          return NM;
        } else if (i == 1 && stat == summary.Max) {
          return NM;
        } else if (i == 1 && stat == summary.Min) {
          return NM;
        }
      });

      expect(input.summary, equals(expected));
    });
  });

  group("summary for Series should", () {
    test("NAs for empty series", () {
      expect(new Series<int, double>.empty().summary(),
          equals(new Series.fromCells(stats.map((s) => new Tuple2(s, NA)))));
    });

    test("summarize dense series", () {
      expect(
          summary(new Series.fromPairs(
              [new Tuple2(0, 1.0), new Tuple2(1, 3.0), new Tuple2(2, 2.0)])),
          equals(new Series.fromPairs([
            new Tuple2(summary.Mean, 2.0),
            new Tuple2(summary.Median, 2.0),
            new Tuple2(summary.Max, 3.0),
            new Tuple2(summary.Min, 1.0)
          ])));
    });

    test("summarize series with NAs", () {
      expect(
          summary(new Series.fromCells([
            new Tuple2(0, Value(1.0)),
            new Tuple2(1, NA),
            new Tuple2(2, Value(3.0)),
            new Tuple2(3, NA),
            new Tuple2(4, Value(2.0)),
            new Tuple2(5, NA)
          ])),
          equals(new Series.fromPairs([
            new Tuple2(summary.Mean, 2.0),
            new Tuple2(summary.Median, 2.0),
            new Tuple2(summary.Max, 3.0),
            new Tuple2(summary.Min, 1.0)
          ])));
    });

    test("summarize series with NMs", () {
      expect(
          summary(new Series.fromCells([
            new Tuple2(0, Value(1.0)),
            new Tuple2(1, NM),
            new Tuple2(2, Value(3.0)),
            new Tuple2(3, NA),
            new Tuple2(4, Value(2.0)),
            new Tuple2(5, NA)
          ])),
          equals(new Series<String, double>.fromCells([
            new Tuple2(summary.Mean, NM),
            new Tuple2(summary.Median, NM),
            new Tuple2(summary.Max, NM),
            new Tuple2(summary.Min, NM)
          ])));
    });
  });
}
