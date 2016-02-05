library frame.test.column;

import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

indexTest() {
  group("Index construction", () {
    checkIndex(Index<String> idx, Iterable<Tuple2<String, int>> pairs) {
      expect(idx.toList(), equals(pairs.toList()));
      expect(pairs.map((p) => p.v1).map((p) => idx.search(p)),
          equals(pairs.map((p) => p.v2)));
    }

    test("empty", () {
      checkIndex(Index.empty[String])();
    });

    test("from keys only", () {
      checkIndex(Index.fromKeys(["a", "b", "c"]),
          [new Tuple("a", 0), new Tuple("b", 1), new Tuple("c", 2)]);
    });

    test("from pairs", () {
      checkIndex(
          new Index([new Tuple("a", 5), new Tuple("c", 0), new Tuple("b", 2)]),
          [new Tuple("a", 5), new Tuple("c", 0), new Tuple("b", 2)]);
    });

    test("key/index array pair", () {
      checkIndex(Index(["a", "c", "b"], [5, 0, 2]),
          [new Tuple("a", 5), new Tuple("c", 0), new Tuple("b", 2)]);
    });

    test("ordered with keys only", () {
      checkIndex(Index.ordered(["a", "b", "c"]),
          [new Tuple("a", 0), new Tuple("b", 1), new Tuple("c", 2)]);
    });

    test("ordered with indices", () {
      checkIndex(Index.ordered(["a", "b", "c"], [5, 0, 2]),
          [new Tuple("a", 5), new Tuple("b", 0), new Tuple("c", 2)]);
    });
  });

  group("Index should", () {
    test("traverse elements in original order", () {
      var idx = Index.fromKeys("c", "a", "b");
      expect(idx.toList().map((i) => i.v1), ["c", "a", "b"]);
    });

    test("return the index of an element in the original order", () {
      var idx = Index.fromKeys("c", "a", "b");
      expect(idx.get("a"), equals(Some(1)));
      expect(idx.get("b"), equals(Some(2)));
      expect(idx.get("c"), equals(Some(0)));
    });

    test("allow access by iteration order", () {
      var idx = Index.fromKeys("c", "a", "b");
      expect(idx.apply(0), new Tuple("c", 0));
      expect(idx.apply(1), new Tuple("a", 1));
      expect(idx.apply(2), new Tuple("b", 2));
    });

    test("foreach iterates in order", () {
      var idx =
          Index([new Tuple("c", 1), new Tuple("a", 3), new Tuple("b", 2)]);
      var bldr = ArrayBuilder.make(String, Int);
      idx.foreach((k, i) {
        bldr += new Tuple(k, i);
      });
      expect(bldr.result(),
          equals([new Tuple("c", 1), new Tuple("a", 3), new Tuple("b", 2)]));
    });

    test("sorted puts things in order", () {
      var idx = Index.fromKeys("c", "a", "b").sorted();
      expect(idx.toList(),
          equals([new Tuple2("a", 1), new Tuple2("b", 2), new Tuple2("c", 0)]));
    });

    test("sorted is a stable sort", () {
      var idx0 = Index.unordered(["c", "b", "a", "c", "b", "a"]).sorted();
      var idx1 = Index.unordered(["a", "a", "b", "b", "c", "c"]).sorted();
      var idx2 = Index.unordered(["c", "c", "b", "b", "a", "a"]).sorted();

      expect(
          idx0.toList(),
          equals([
            new Tuple2("a", 2),
            new Tuple2("a", 5),
            new Tuple2("b", 1),
            new Tuple2("b", 4),
            new Tuple2("c", 0),
            new Tuple2("c", 3)
          ]));
      expect(
          idx1.toList(),
          equals([
            new Tuple2("a", 0),
            new Tuple2("a", 1),
            new Tuple2("b", 2),
            new Tuple2("b", 3),
            new Tuple2("c", 4),
            new Tuple2("c", 5)
          ]));
      expect(
          idx2.toList(),
          equals([
            new Tuple2("a", 4),
            new Tuple2("a", 5),
            new Tuple2("b", 2),
            new Tuple2("b", 3),
            new Tuple2("c", 0),
            new Tuple2("c", 1)
          ]));
    });

    test("get all rows for single key as Index", () {
      var idx = Index([
        new Tuple2("b", 0),
        new Tuple2("c", 2),
        new Tuple2("a", 0),
        new Tuple2("b", 1)
      ]);
      expect(idx.getAll("b"),
          equals(Index([new Tuple2("b", 0), new Tuple2("b", 1)])));
    });

    test("be equal to any empty Index if it is empty", () {
      expect(new Index<String>(), equals(new Index<String>()));
    });

    test("be equal when they have same key/row pairs", () {
      idx() => new Index([
            new Tuple2("a", 0),
            new Tuple2("b", 3),
            new Tuple2("c", 2),
            new Tuple2("c", 0)
          ]);
      expect(idx(), equals(idx()));
    });

    test("be equal only if key/row pairs are equal", () /*! check*/ {
      Index<String> idx0;
      Index<String> idx1;
      expect(idx0 == idx1, equals(idx0.to[Vector] == idx1.to[Vector]));
    });
  });

  group("Index.Grouper should", () {
    var grouper = new TestGrouper<String>();

    test("group empty index", () {
      var idx = new Index<String>();
      expect(Index.group(idx, grouper).groups,
          equals(new Vector<Tuple2<String, Int>>.empty()));
    });

    test("trivial grouping", () {
      var idx = Index.fromKeys("a", "a", "a");
      var expected = [
        [new Tuple2("a", 0), new Tuple2("a", 1), new Tuple2("a", 2)]
      ];
      expect(Index.group(idx, grouper).groups, equals(expected));
    });

    test("group in-order elements", () {
      var idx = Index.fromKeys("a", "b", "c");
      var expected = [
        [new Tuple2("a", 0)],
        [new Tuple2("b", 1)],
        [new Tuple2("c", 2)]
      ];
      expect(Index.group(idx, grouper).groups, equals(expected));
    });

    test("group out-of-order elements", () {
      var idx = Index.fromKeys("a", "b", "a", "b");
      var expected = [
        [new Tuple2("a", 0), new Tuple2("a", 2)],
        [new Tuple2("b", 1), new Tuple2("b", 3)]
      ];
      expect(Index.group(idx, grouper).groups, equals(expected));
    });
  });

  group("reset indices", () {
    test("do nothing when indices aren't specified", () {
      var idx0 = Index.fromKeys("a", "c", "b");
      expect(idx0.resetIndices, equals(idx0));

      var idx1 = Index.fromKeys("a", "b", "c");
      expect(idx1.resetIndices(), equals(idx1));
    });

    expect("reset indices in traversal order", () /*! check*/ {
      List<Tuple<String, int>> pairs;
      expect(Index(pairs).resetIndices(),
          equals(Index(pairs.map((p) => p.v1).toArray())));
    });
  });

  group("Index.Cogrouper", () {
    var cogrouper = new TestCogrouper<String>();

    test("cogroup empty indices", () {
      var idx = new Index<String>();
      var expected = new Vector<Cogroup<String>>.empty();
      expect(Index.cogroup(idx, idx, cogrouper).groups(), equals(expected));
    });

    test("cogroup with 1 group on left", () {
      var lhs = Index.fromKeys("a", "a");
      var rhs = new Index<String>();
      var expected = new List<Cogroup<String>>.from([
        [new Tuple2("a", 0), new Tuple2("a", 1)],
        []
      ]);
      expect(Index.cogroup(lhs, rhs, cogrouper).groups(), equals(expected));
    });

    test("cogroup with 1 group on right", () {
      var lhs = new Index<String>();
      var rhs = new Index.fromKeys("a", "a");
      var expected = new List<Cogroup<String>>.from([
        [],
        [new Tuple2("a", 0), new Tuple2("a", 1)]
      ]);
      expect(Index.cogroup(lhs, rhs, cogrouper).groups(), equals(expected));
    });

    test("cogroup with 1 shared group", () {
      var lhs = Index.fromKeys("a", "a");
      var rhs = Index([new Tuple2("a", 2)]);
      var expected = new List<Cogroup<String>>.from([
        [new Tuple2("a", 0), new Tuple2("a", 1)],
        [new Tuple2("a", 2)]
      ]);
      expect(Index.cogroup(lhs, rhs, cogrouper).groups(), equals(expected));
    });

    test("cogroup missing group on left", () {
      var lhs = Index.fromKeys("a", "a");
      var rhs = Index.fromKeys("a", "b");
      var expected = new List<Cogroup<String>>([
        [new Tuple2("a", 0), new Tuple2("a", 1)],
        [new Tuple2("a", 0)],
        [
          [],
          [new Tuple2("b", 1)]
        ]
      ]);
      expect(Index.cogroup(lhs, rhs, cogrouper).groups(), equals(expected));
    });

    test("cogroup in-order indices", () {
      var lhs = Index.fromKeys("a", "a", "b");
      var rhs = Index.fromKeys("a", "b", "c");
      var expected = new List<Cogroup<String>>.from([
        [
          [new Tuple2("a", 0), new Tuple2("a", 1)],
          [new Tuple2("a", 0)]
        ],
        [
          [
            new Tuple2("b", 2),
            [new Tuple2("b", 1)]
          ]
        ],
        [
          [],
          [new Tuple2("c", 2)]
        ]
      ]);
      expect(Index.cogroup(lhs, rhs)(cogrouper).groups(), equals(expected));
    });

    test("cogroup out-of-order indices", () {
      var lhs = Index.fromKeys("a", "b", "a", "b");
      var rhs = Index.fromKeys("c", "a", "b", "c");
      var expected = new List<Cogroup<String>>.from([
        [
          [new Tuple2("a", 0), new Tuple2("a", 2)],
          [new Tuple2("a", 1)]
        ],
        [
          [new Tuple2("b", 1), new Tuple2("b", 3)],
          [new Tuple2("b", 2)]
        ],
        [
          [],
          [new Tuple2("c", 0), new Tuple2("c", 3)]
        ]
      ]);
      expect(Index.cogroup(lhs, rhs, cogrouper).groups(), equals(expected));
    });
  });
}

class State {
  Vector<List<Tuple2<K, int>>> groups;
  State(this.groups);
}

class TestGrouper<K> extends Grouper<K> {
  init() => new State(Vector.empty());

  State group(
      State state, List<K> keys, List<int> indices, int start, int end) {
//    return new State(state.groups :+ (keys.zip(indices)).drop(start).take(end - start).toList())
  }
}

//type Cogroup[K] = (List[(K, Int)], List[(K, Int)])

class State2 {
  Vector<Cogroup<K>> groups;
}

class TestCogrouper<K> extends Cogrouper<K> {
  init() => State(Vector.empty);

  State2 cogroup(State2 state, List<K> lKeys, List<int> lIdx, int lStart,
      int lEnd, List<K> rKeys, List<int> rIdx, int rStart, int rEnd) {
    var lGroup = (lKeys.zip(lIdx)).drop(lStart).take(lEnd - lStart).toList();
    var rGroup = (rKeys.zip(rIdx)).drop(rStart).take(rEnd - rStart).toList();
//    return new State2(state.groups :+ (lGroup, rGroup))
  }
}
