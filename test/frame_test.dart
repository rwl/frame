library frame.test.column;

import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

frameTest() {
  var f0 = Frame.fromRows([
    ["a", 1, HNil],
    ["b", 2, HNil],
    ["c", 3, HNil]
  ]);
  var f1 = Frame.fromRows([
    ["a", 3, HNil],
    ["b", 2, HNil],
    ["c", 1, HNil]
  ]);
  var f2 = Frame.fromRows([
    ["a", 1, HNil],
    ["b", 2, HNil],
    ["b", 3, HNil]
  ]);

  var f3 =
      Series([new Tuple2(1, 3), new Tuple2(2, 2), new Tuple2(2, 1)]).toFrame(0);
  var f4 =
      Series([new Tuple2(1, 3), new Tuple2(2, 2), new Tuple2(2, 1)]).toFrame(1);
  var f5 =
      Series([new Tuple2(2, 3), new Tuple2(2, 2), new Tuple2(3, 1)]).toFrame(1);
  var f6 = Series([new Tuple2(2, 2), new Tuple2(2, 1)]).toFrame(1);

  var s0 =
      Series([new Tuple2(0, "s3"), new Tuple2(1, "s2"), new Tuple2(2, "s1")]);
  var s1 =
      Series([new Tuple2(1, "s3"), new Tuple2(2, "s2"), new Tuple2(2, "s1")]);

  var homogeneous = Frame.fromRows([
    [1.0, 2.0, 3.0, HNil],
    [0.5, 1.0, 1.5, HNil],
    [0.25, 0.5, 0.75, HNil]
  ]);
  var people = Frame.fromRows([
    ["Bob", 32, "Manager", HNil],
    ["Alice", 24, "Employee", HNil],
    ["Charlie", 44, "Employee", HNil]
  ])
      .withColIndex(Index.fromKeys("Name", "Age", "Level"))
      .withRowIndex(Index.fromKeys("Bob", "Alice", "Charlie"));

  group("Frame should", () {
    test("be fill-able", () {
      var f = Frame.fill(range(1, 4), range(4, 6), (i, j) {
        var k = i + j;
        return (k % 2 == 0) ? NA : Value(k);
      });

      expect(
          f,
          equals(Frame.mergeColumns([
            new Tuple2(
                4,
                Series.fromCells([
                  new Tuple2(1, Value(5)),
                  new Tuple2(2, NA),
                  new Tuple2(3, Value(7))
                ])),
            new Tuple2(
                5,
                Series.fromCells([
                  new Tuple2(1, NA),
                  new Tuple2(2, Value(7)),
                  new Tuple2(3, NA)
                ]))
          ])));
    });

    test("have sane equality", () {
      expect(f0, equals(f0));
      expect(f0, isNot(equals(f1)));
      expect(f1, isNot(equals(f0)));
      expect(f0.column[String](0).toFrame("abc"),
          equals(f1.column[String](0).toFrame("abc")));
      expect(f0.column[Int](1).toFrame("123"),
          isNot(equals(f1.column[Int](1).toFrame("123"))));
    });

    test("have sane hashCode", () {
      expect(f0.hashCode, equals(f0.hashCode));
      expect(f0.hashCode, isNot(equals(f1.hashCode)));
      expect(f1.hashCode, isNot(equals(f0.hashCode)));
      expect(f0.column[String](0).toFrame("abc").hashCode,
          equals(f1.column[String](0).toFrame("abc").hashCode));
      expect(f0.column[Int](1).toFrame("123").hashCode,
          isNot(equals(f1.column[Int](1).toFrame("123").hashCode)));
    });

    test("sort columns", () {
      expect(
              people.sortColumns(),
              equals(Frame.fromRows([
                [32, "Manager", "Bob", HNil],
                [24, "Employee", "Alice", HNil],
                [44, "Employee", "Charlie", HNil]
              ])))
          .withColIndex(Index.fromKeys("Age", "Level", "Name"))
          .withRowIndex(Index.fromKeys("Bob", "Alice", "Charlie"));
    });

    test("sort rows", () {
      expect(
          people.sortRows,
          equals(Frame.fromRows([
            ["Alice", 24, "Employee", HNil],
            ["Bob", 32, "Manager", HNil],
            ["Charlie", 44, "Employee", HNil]
          ])
              .withColIndex(Index.fromKeys("Name", "Age", "Level"))
              .withRowIndex(Index.fromKeys("Alice", "Bob", "Charlie"))));
    });

    test("use new row index", () {
      expect(
          f0.withRowIndex(
              Index([new Tuple2(0, 2), new Tuple2(1, 0), new Tuple2(2, 1)])),
          equals(Frame.fromRows([
            ["c", 3, HNil],
            ["a", 1, HNil],
            ["b", 2, HNil]
          ])));
      expect(
          f0.withRowIndex(
              Index([new Tuple2(0, 0), new Tuple2(1, 0), new Tuple2(2, 0)])),
          equals(Frame.fromRows([
            ["a", 1, HNil],
            ["a", 1, HNil],
            ["a", 1, HNil]
          ])));
      expect(
          f0.withRowIndex(Index([new Tuple2(0, 2)])),
          equals(Frame.fromRows([
            ["c", 3, HNil]
          ])));
      expect(f0.withRowIndex(new Index<int>.empty()),
          equals(new Frame<int, int>.empty().withColIndex(f0.colIndex)));
    });

    test("use new column index", () {
      expect(
          f0.withColIndex(Index([new Tuple2(0, 1), new Tuple2(1, 0)])),
          equals(Frame.fromRows([
            [1, "a", HNil],
            [2, "b", HNil],
            [3, "c", HNil]
          ])));
      expect(
          f0.withColIndex(Index([new Tuple2(0, 0), new Tuple2(1, 0)])),
          equals(Frame.fromRows([
            ["a", "a", HNil],
            ["b", "b", HNil],
            ["c", "c", HNil]
          ])));
      expect(
          f0.withColIndex(Index([new Tuple2(0, 1)])),
          equals(Frame.fromRows([
            [1, HNil],
            [2, HNil],
            [3, HNil]
          ])));
      expect(f0.withColIndex(new Index<int>.empty()),
          equals(new Frame<HNil, int>.fromRows([HNil, HNil, HNil])));
    });

    test("have trivial column/row representation for empty Frame", () {
      var frame = new Frame<String, String>.empty();
      expect(frame.columnsAsSeries,
          equals(new Series<String, UntypedColumn>.empty()));
      expect(frame.rowsAsSeries,
          equals(new Series<String, UntypedColumn>.empty()));
    });

    test("be representable as columns", () {
      var series = f0.columnsAsSeries(mapValues((col) {
        return Series(f0.rowIndex, col.cast[Any]);
      }));

      expect(
          series,
          equals(Series([
            [
              new Tuple2(
                  0,
                  Series(new Tuple2(0, "a"), new Tuple2(1, "b"),
                      new Tuple2(2, "c")))
            ],
            [
              new Tuple2(1,
                  Series(new Tuple2(0, 1), new Tuple2(1, 2), new Tuple2(2, 3)))
            ]
          ])));
    });

    test("be representable as rows", () {
      var series = f0.rowsAsSeries(mapValues((col) {
        return new Series(f0.colIndex, col.cast[Any]);
      }));

      expect(
          series,
          equals(Series([
            [new Tuple2(0, Series(new Tuple2(0, "a"), new Tuple2(1, 1)))],
            [new Tuple2(1, Series(new Tuple2(0, "b"), new Tuple2(1, 2)))],
            [new Tuple2(2, Series(new Tuple2(0, "c"), new Tuple2(1, 3)))]
          ])));
    });
  });

  group("Frame merges should", () {
    // these cases work as expected... tacking on a new column...
    test("inner merge with frame of same row index", () {
      expect(
          f3.merge(f4, Merge.Inner),
          equals(Frame.fromRows([
            [3, 3, HNil],
            [2, 2, HNil],
            [1, 1, HNil]
          ]).withRowIndex(Index([1, 2, 2]))));
    });

    test("outer merge with frame of same row index", () {
      expect(
          f3.merge(f4, Merge.Outer),
          equals(Frame.fromRows([
            [3, 3, HNil],
            [2, 2, HNil],
            [1, 1, HNil]
          ]).withRowIndex(Index([1, 2, 2]))));
    });

    test("inner merge with an offset index with duplicates", () {
      expect(
          f3.merge(f5, Merge.Inner),
          equals(Frame.fromRows([
            [2, 3, HNil],
            [1, 2, HNil]
          ]).withRowIndex(Index([2, 2]))));
    });

    test("outer merge with an offset index with duplicates", () {
      expect(
          f3.merge(f5, Merge.Outer),
          equals(Frame.fromRows([
            [3, NA, HNil],
            [2, 3, HNil],
            [1, 2, HNil],
            [NA, 1, HNil]
          ]).withRowIndex(Index([1, 2, 2, 3]))));
    });

    test("inner merge with a smaller index with duplicates", () {
      expect(
          f3.merge(f6, Merge.Inner),
          equals(Frame.fromRows([
            [2, 2, HNil],
            [1, 1, HNil]
          ]).withRowIndex(Index([2, 2]))));
    });

    test("outer merge with a smaller index with duplicates", () {
      expect(
          f3.merge(f6)(Merge.Outer),
          equals(Frame.fromRows([
            [3, NA, HNil],
            [2, 2, HNil],
            [1, 1, HNil]
          ]).withRowIndex(Index([1, 2, 2]))));
    });

    test("merge with a series", () {
      expect(
          f3.merge(1, s1, Merge.Inner),
          equals(Frame.fromRows([
            [3, "s3", HNil],
            [2, "s2", HNil],
            [1, "s1", HNil]
          ]).withRowIndex(Index([1, 2, 2]))));
    });
  });

  group("Frame joins should", () {
    test("inner join with empty frame", () {
      var e = new Frame<int, int>.empty();
      expect(f0.join(e, Join.Inner),
          equals(f0.withRowIndex(new Index<int>.empty())));
      expect(e.join(f0, Join.Inner),
          equals(f0.withRowIndex(new Index<int>.empty())));
      expect(e.join(e, Join.Inner), equals(e));
    });

    test("inner join with series", () {
      expect(
          f0.join(2, s0)(Join.Inner),
          equals(Frame.fromRows([
            ["a", 1, "s3", HNil],
            ["b", 2, "s2", HNil],
            ["c", 3, "s1", HNil]
          ])));
    });

    test("inner join with self", () {
      expect(
          f0.join(f0, Join.Inner),
          equals(Frame.fromRows([
            ["a", 1, "a", 1, HNil],
            ["b", 2, "b", 2, HNil],
            ["c", 3, "c", 3, HNil]
          ]).withColIndex(Index.fromKeys([0, 1, 0, 1]))));
    });

    test("inner join only matching rows", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var b = Frame.fromRows([
        [2.0, HNil],
        [3.0, HNil]
      ]).withRowIndex(Index.fromKeys("b", "c"));
      var c = Frame.fromRows([
        [2, 2.0, HNil]
      ]).withRowIndex(Index.fromKeys("b")).withColIndex(Index.fromKeys(0, 0));

      expect(a.join(b, Join.Inner), equals(c));
    });

    test("inner join forms cross-product of matching rows", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "a"));
      var b = Frame.fromRows([
        [2.0, HNil],
        [3.0, HNil]
      ]).withRowIndex(Index.fromKeys("a", "a"));
      var c = Frame.fromRows([
        [1, 2.0, HNil],
        [1, 3.0, HNil],
        [2, 2.0, HNil],
        [2, 3.0, HNil]
      ])
          .withRowIndex(Index.fromKeys("a", "a", "a", "a"))
          .withColIndex(Index.fromKeys(0, 0));

      expect(a.join(b, Join.Inner), equals(c));
    });

    test("left join keeps left mismatched rows", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var b = Frame.fromRows([
        [2.0, HNil],
        [3.0, HNil]
      ]).withRowIndex(Index.fromKeys("b", "c"));
      var c = Frame.mergeColumns([
        new Tuple2(
            0,
            Series.fromCells(
                new Tuple2("a", Value(1)), new Tuple2("b", Value(2)))),
        new Tuple2(0,
            Series.fromCells(new Tuple2("a", NA), new Tuple2("b", Value(2.0))))
      ]);
      expect(a.join(b, Join.Left), equals(c));
    });

    test("left join with empty frame", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var e = new Frame<String, int>.empty();
      expect(a.join(e, Join.Left), equals(a));
      expect(e.join(a, Join.Left), equals(e.withColIndex(Index.fromKeys(0))));
    });

    test("right join keeps right mismatched rows", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var b = Frame.fromRows([
        [2.0, HNil],
        [3.0, HNil]
      ]).withRowIndex(Index.fromKeys("b", "c"));
      var c = Frame.mergeColumns([
        new Tuple2(0,
            Series.fromCells(new Tuple2("b", Value(2)), new Tuple2("c", NA))),
        new Tuple2(
            0,
            Series.fromCells(
                new Tuple2("b", Value(2.0)), new Tuple2("c", Value(3.0))))
      ]);
      expect(a.join(b, Join.Right), equals(c));
    });

    test("right join with empty frame", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var e = new Frame<String, int>.empty();
      expect(a.join(e, Join.Right), equals(e.withColIndex(Index.fromKeys(0))));
      expect(e.join(a, Join.Right), equals(a));
    });

    test("outer join keeps all rows", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var b = Frame.fromRows([
        [2.0, HNil],
        [3.0, HNil]
      ]).withRowIndex(Index.fromKeys("b", "c"));
      var c = Frame.mergeColumns([
        new Tuple2(
            0,
            Series.fromCells(new Tuple2("a", Value(1)),
                new Tuple2("b", Value(2)), new Tuple2("c", NA))),
        new Tuple2(
            0,
            Series.fromCells(new Tuple2("a", NA), new Tuple2("b", Value(2.0)),
                new Tuple2("c", Value(3.0))))
      ]);
      expect(a.join(b, Join.Outer), equals(c));
    });

    test("outer join with empty frame", () {
      var a = Frame.fromRows([
        [1, HNil],
        [2, HNil]
      ]).withRowIndex(Index.fromKeys("a", "b"));
      var e = new Frame<String, int>.empty();
      expect(a.join(e, Join.Outer), equals(a));
      expect(e.join(a, Join.Outer), equals(a));
    });
  });

  group("mapRowGroups should", () {
    test("not modify frame for identity", () {
      expect(f0.mapRowGroups((_, f) => f), equals(f0));
      expect(f1.mapRowGroups((_, f) => f), equals(f1));
    });

    var dups = Frame.fromRows([
      [1, 2.0, HNil],
      [2, 0.5, HNil],
      [3, 1.0, HNil],
      [4, 1.0, HNil],
      [5, 8.9, HNil],
      [6, 9.2, HNil]
    ]).withRowIndex(Index.fromKeys("a", "a", "b", "c", "c", "c"));

    test("reduce groups", () {
      dups.mapRowGroups((row, f) {
        var reduced = f.reduceFrame(reduce.Sum[Double]).to[List];
        ColOrientedFrame(Index.fromKeys(row), Series(reduced.map((key, value) {
          return new Tuple2(key, TypedColumn(Column(value)));
        })));
      }, equals(dups.reduceFrameByKey(reduce.Sum[Double])));
    });

    test("replace groups with constant", () {
      var cnst = Frame.fromRows([
        ["repeat", HNil]
      ]);
      expect(
          dups.mapRowGroups((_, f) => cnst),
          equals(Frame.fromRows([
            ["repeat", HNil],
            ["repeat", HNil],
            ["repeat", HNil]
          ]).withRowIndex(Index.fromKeys(0, 0, 0))));
    });
  });

  group("Frame should", () {
    test("get row as HList", () {
      expect(f0.get(Cols(0, 1).as[[String, Int, HNil]])(0),
          equals(Value(["a", 1, HNil])));
      expect(f0.get(Cols(0, 1).as[[String, Int, HNil]])(1),
          equals(Value(["b", 2, HNil])));
      expect(f0.get(Cols(0, 1).as[[String, Int, HNil]])(2),
          equals(Value(["c", 3, HNil])));
      expect(f0.get(Cols(0, 1).as[[String, Int, HNil]])(3), equals(NA));
      expect(f0.get(Cols(0).as[[String, HNil]])(0), equals(Value(["a", HNil])));
      expect(f0.get(Cols(1).as[[Int, HNil]])(2), equals(Value([3, HNil])));
    });

    test("convert to series", () {
      expect(
          f0.get(Cols(0).as[String]),
          equals(Series(
              new Tuple2(0, "a"), new Tuple2(1, "b"), new Tuple2(2, "c"))));
      expect(f0.get(Cols(0).as[Int]),
          equals(Series(Index.fromKeys(0, 1, 2), Column[Int](NM, NM, NM))));
      expect(f0.get(Cols(1).as[Int]),
          equals(Series(new Tuple2(0, 1), new Tuple2(1, 2), new Tuple2(2, 3))));
      expect(
          f0.get(Cols(0, 1).as[[String, Int, HNil]]),
          equals(Series(
              new Tuple2(0, (["a", 1, HNil])),
              new Tuple2(1, (["b", 2, HNil])),
              new Tuple2(2, (["c", 3, HNil])))));
    });

    test("map to series", () {
      expect(
          f0.map(Cols(1).as[Int], 2, (a) => a + 1),
          equals(Frame.fromRows([
            ["a", 1, 2, HNil],
            ["b", 2, 3, HNil],
            ["c", 3, 4, HNil]
          ])));
      expect(
          f0.map(Cols(0).as[String], 2, (a) => 42),
          equals(Frame.fromRows([
            ["a", 1, 42, HNil],
            ["b", 2, 42, HNil],
            ["c", 3, 42, HNil]
          ])));
      expect(
          f0.map(Cols(1, 0).as[new Tuple2<Int, String>()], 2, (x, y) => y + x),
          equals(Frame.fromRows([
            ["a", 1, "a1", HNil],
            ["b", 2, "b2", HNil],
            ["c", 3, "c3", HNil]
          ])));
    });

    test("map with index to series", () {
      expect(
          f0.mapWithIndex(Cols(0).as[String], 2, (a, b) => a + b),
          equals(Frame.fromRows([
            ["a", 1, "0a", HNil],
            ["b", 2, "1b", HNil],
            ["c", 3, "2c", HNil]
          ])));
      expect(
          f0.mapWithIndex(Cols(1).as[Int], 2, (a, b) => a + b),
          equals(Frame.fromRows([
            ["a", 1, 1, HNil],
            ["b", 2, 3, HNil],
            ["c", 3, 5, HNil]
          ])));
    });

    test("filter whole frame", () {
      expect(
          f0.filter(Cols(1).as[Int], (a) => a % 2 == 0),
          equals(
              Frame.fromRows(["b", 2, HNil]).withRowIndex(Index.fromKeys(1))));
    });

    test("group by column values", () {
      expect(f0.group(Cols(0).as[String]),
          equals(f0.withRowIndex(Index.fromKeys("a", "b", "c"))));
      expect(
          f0.group(Cols(1).as[Int].map((i) => -i)),
          equals(f0.withRowIndex(
              Index(-new Tuple2(3, 2), -new Tuple2(2, 1), -new Tuple2(1, 0)))));
//      expect(f2.group(Cols(0).as[String]), equals(f2.withRowIndex(Index(("a",0), ("b",2), ("b",1)))
    });
  });

  group("reduceFrameWithCol should", () {
    test("reduce with last", () {
      expect(
          f0.reduceFrameWithCol /*[String, Int, (String, Int)]*/ (
              0, reduce.Last),
          equals(Series(new Tuple2(1, new Tuple2("c", 3)))));
    });
  });

  group("map should", () {
    test("append column when to is new", () {
      expect(
          f0.map(Cols(1).as[Int], 2, (a) => a + 2),
          equals(Frame.fromRows([
            ["a", 1, 3, HNil],
            ["b", 2, 4, HNil],
            ["c", 3, 5, HNil]
          ])));
    });

    test("replace column when `to` exists", () {
      expect(
          f0.map(Cols(1).as[Int], 1, (a) => a + 2),
          equals(Frame.fromRows([
            ["a", 3, HNil],
            ["b", 4, HNil],
            ["c", 5, HNil]
          ])));
    });
  });

  group("reduce should", () {
    test("reduce rows", () {
      expect(
          f0.reduce(Cols(1).as[Double], 2, reduce.Mean),
          equals(f0.merge(
              2,
              Series(
                  new Tuple2(0, 2.0), new Tuple2(1, 2.0), new Tuple2(2, 2.0)),
              Merge.Outer)));
    });

    test("reduce cols", () {
      expect(
          f0.transpose.reduce(Rows(1).as[Double], 2, reduce.Mean),
          equals(f0
              .merge(
                  2,
                  Series(new Tuple2(0, 2.0), new Tuple2(1, 2.0),
                      new Tuple2(2, 2.0)),
                  Merge.Outer)
              .transpose()));
    });

    test("replace column when `to` exists", () {
      expect(
          f0.reduce(Cols(1).as[Int], 1, reduce.Sum),
          equals(Frame.fromRows([
            ["a", 6, HNil],
            ["b", 6, HNil],
            ["c", 6, HNil]
          ])));

      expect(
          f0.transpose.reduce(Rows(1).as[Int], 1, reduce.Sum),
          equals(Frame.fromColumns([
            ["a", 6, HNil],
            ["b", 6, HNil],
            ["c", 6, HNil]
          ])));
    });

    test("respect NMs in reducer", () {
      var f = Series
          .fromCells(new Tuple2(1, Value(1)), new Tuple2(2, NM),
              new Tuple2(3, Value(3)))
          .toFrame("x");
      expect(
          f.reduce(Cols("x").as[Int], "y", reduce.Sum),
          equals(f.merge(
              "y",
              Series(new Tuple2(1, NM), new Tuple2(2, NM), new Tuple2(3, NM)),
              Merge.Outer)));
    });

    test("respect NAs in reducer", () {
      var f = new Series<int, int>.fromCells(
          new Tuple2(1, NA), new Tuple2(2, NA)).toFrame("x");
      expect(
          f.reduce(Cols("x").as[Int], "y", reduce.Max),
          equals(f.merge(
              "y", Series(new Tuple2(1, NA), new Tuple2(2, NA)), Merge.Outer)));
    });
  });

  group("reduceByKey should", () {
    var f = Frame.mergeColumns(
        new Tuple2(
            "x",
            Series.fromCells(new Tuple2(0, Value(1)), new Tuple2(2, Value(5)),
                new Tuple2(2, Value(6)))),
        new Tuple2(
            "y",
            Series.fromCells(new Tuple2(0, Value(2)), new Tuple2(0, Value(3)),
                new Tuple2(1, NM), new Tuple2(1, Value(2)))));

    test("reduce rows/cols", () {
      expect(
          f.reduceByKey(Cols("x").as[Int], "z", reduce.Sum),
          equals(f.join(
              "z",
              Series.fromCells(new Tuple2(0, Value(1)), new Tuple2(1, Value(0)),
                  new Tuple2(2, Value(11))),
              Join.Outer)));

      expect(
          f.transpose.reduceByKey(Rows("x").as[Int], "z", reduce.Sum),
          equals(f
              .join(
                  "z",
                  Series.fromCells(new Tuple2(0, Value(1)),
                      new Tuple2(1, Value(0)), new Tuple2(2, Value(11))),
                  Join.Outer)
              .transpose()));
    });

    test("respect NMs from reducer", () {
      expect(
          f.reduceByKey(Cols("y").as[Int], "z", reduce.Sum),
          equals(f.join(
              "z",
              Series.fromCells(new Tuple2(0, Value(5)), new Tuple2(1, NM),
                  new Tuple2(2, Value(0))),
              Join.Outer)));
    });

    test("respect NAs from reducer", () {
      expect(
          f.reduceByKey(Cols("x").as[Int], "z", reduce.Max),
          equals(f.join(
              "z",
              Series.fromCells(new Tuple2(0, Value(1)), new Tuple2(1, NA),
                  new Tuple2(2, Value(6))),
              Join.Outer)));
    });
  });

  group("appendRows should", () {
    test("append rows to empty frame", () {
      expect(new Frame<int, int>.empty().appendRows(f0), equals(f0));
      expect(f0.appendRows(new Frame<int, int>.empty()), equals(f0));
    });

    test("append 2 simple frames with same columns", () {
      expect(
          f0.appendRows(f1),
          equals(Frame.fromRows([
            ["a", 1, HNil],
            ["b", 2, HNil],
            ["c", 3, HNil],
            ["a", 3, HNil],
            ["b", 2, HNil],
            ["c", 1, HNil]
          ]).withRowIndex(Index([0, 1, 2, 0, 1, 2]))));
    });

    test("append 2 simple frames with different columns", () {
      var a = Frame.fromRows([
        ["a", 1, HNil],
        ["b", 2, HNil],
        ["c", 3, HNil]
      ]);
      var b = Frame.fromRows([
        [9, 4.0, HNil],
        [8, 5.0, HNil],
        [7, 6.0, HNil]
      ]).withColIndex(Index([1, 2]));

      var col0 = Column(Value("a"), Value("b"), Value("c"), NA, NA, NA);
      var col1 =
          Column(Value(1), Value(2), Value(3), Value(9), Value(8), Value(7));
      var col2 = Column(NA, NA, NA, Value(4.0), Value(5.0), Value(6.0));
      expect(
          a.appendRows(b),
          equals(ColOrientedFrame(
              Index([0, 1, 2, 0, 1, 2]),
              Series(
                  new Tuple2(0, TypedColumn(col0)),
                  new Tuple2(1, TypedColumn(col1)),
                  new Tuple2(2, TypedColumn(col2))))));
    });

    test("append frame rows with same column oriented schema", () {
      var genFrame = genColOrientedFrame /*[Int, String](arbitrary[Int])*/ (
          new Tuple2("a", arbitrary[String]),
          new Tuple2("b", arbitrary[Int]),
          new Tuple2("c", arbitrary[Double]));

//      forAll(Gen.zip(genFrame, genFrame)) { case (f0, f1) =>
//        var rows0 = f0.get(Cols("a", "b", "c").as[(String, Int, Double)])
//        var rows1 = f1.get(Cols("a", "b", "c").as[(String, Int, Double)])
//        var index = Index(rows0.index.keys ++ rows1.index.keys)
//        var values = rows0.values ++ rows1.values
//        var expected = Frame.fromRows(values: _*).withRowIndex(index).withColIndex(Index(Array("a", "b", "c")))
//        f0.appendRows(f1), equals(expected
//      }
    });
  });
}
