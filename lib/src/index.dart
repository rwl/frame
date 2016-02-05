/*
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

part of frame;

abstract class Index<K> extends ListBase<Tuple2<K, int>> {
  Order<K> order;
  ClassTag<K> classTag;

  Index();

  /**
   * Returns an empty `Index` with the same key type as this.
   */
  Index<K> empty() => Index.empty();

  /**
   * Returns the number of key/row pairs in this index.
   */
  int size();

  /**
   * Returns an iterator, in traversal order, over the key/row pairs in this
   * index.
   */
  Iterator<Tuple2<K, int>> iterator();

  /**
   * Returns the key/row pair at position `i`.
   */
  Tuple2<K, int> apply(int i) => new Tuple2(keyAt(i), indexAt(i));

  /**
   * Returns the key at position `i`.
   */
  K keyAt(int i);

  /**
   * Returns the row at position `i`.
   */
  int indexAt(int i);

  /**
   * Iterate over all key/row pairs in traversal order, calling `f` with each
   * pair for its side-effects.
   */
  Unit foreach(f(Tuple2<K, int> a));

  int findStart_(K k, int i) {
    if (i > 0 && keys(i - 1) == k) {
      return findStart(k, i - 1);
    } else {
      return i;
    }
  }

  int findEnd_(K k, int i) {
    if (i < keys.length && keys(i) == k) {
      return findEnd(k, i + 1);
    } else {
      return i;
    }
  }

  /**
   * Returns the index (in traversal order) of the first key/row pair whose key
   * is `k`. If no such key exist in this index, then the this returns
   * `-i - 1`, where `i` is the position in the index where `k` could be
   * inserted while still maintaining sorted order.
   *
   * @param k the key to search for
   */
  int search(K k) {
    var i = Searching.search(keys, k);
    if (i < 0) {
      var j = -i - 1;
      if (j < indices.length) {
        return -indices(-i - 1) - 1;
      } else {
        return -j - 1;
      }
    } else {
      return indices(findStart(k, i));
    }
  }

  /**
   * Returns the position of the first key/row pair with key `k`. If no key/row
   * pair with key `k` exist in this index, then `None` is returned.
   *
   * @param k the key to search for
   */
  Option<int> get(K k) {
    var i = search(k);
    return (i >= 0) ? Some(i) : None;
  }

  /**
   * Returns an index with just the key/row pairs whose key is `k`. If there
   * are no such pairs, then an empty index is returned.
   *
   * @param k the key of the key/row pairs returned
   */
  Index<K> getAll(K k) {
    var i = Searching.search(keys, k);
    if (i >= 0) {
      val lb = findStart(k, i);
      val ub = findEnd(k, i + 1);
      return Index.ordered(keys.slice(lb, ub), indices.slice(lb, ub));
    } else {
      return new Index<K>.empty();
    }
  }

  /**
   * Returns an index whose traversal order is the reverse of this one's.
   */
  Index<K> reverse() {
    var keys0 = new List<K>(keys.length);
    var indices0 = new List<int>(indices.length);
    for (var i = 0; i < keys.length; i += 1) {
      var j = keys.length - i - 1;
      keys0[i] = keyAt(j);
      indices0[i] = indexAt(j);
    }
    return new Index(keys0, indices0);
  }

  /**
   * Returns this [[Index]] in sorted order, by its keys. This operation runs
   * in constant time, since it simply "forgets" the traversal ordering, if
   * one exists.
   */
  OrderedIndex<K> sorted() => Index.ordered(keys, indices);

  /**
   * Returns a copy of this Index, but whose rows have been replaced with their
   * index in the traversal order instead. For example,
   *
   * {{{
   * val index = Index("b" -> 32, "c" -> 9, "a" -> -23)
   * assert(index.resetIndices == Index("b" -> 0, "c" -> 1, "a" -> 2))
   * }}}
   */
  Index<K> resetIndices();

  /**
   * Returns 2 arrays that match the key/row pairings, in traversal order.
   * Namely, the following invariant should hold:
   *
   * {{{
   * val index: Index<K> = ...
   * val (keys, indices) = index.unzip
   * val copy = Index(keys, indices)
   * assert(index == copy)
   * }}}
   */
  Tuple2<List<K>, List<int>> unzip();

  /**
   * Returns `true` if this index is in sorted order.
   */
  bool isOrdered();

  // These must contain both the keys and the indices, in sorted order.
  List<K> keys_();
  List<int> indices_();
  Index<K> withIndices_(List<int> ix);

//  @override
  Unit forEach(f(Tuple2<K, int> a)) => foreach(Function.untupled(f));

//  @override
  Index<K> seq() => this;

//  override
  mutable.Builder<Tuple2<K, int>, Index<K>> newBuilder() =>
      new Index.IndexBuilder();

  @override
  bool operator ==(that) {
//    switch (that) {
//    case (that: Index[_]) =>
//      if (this eq that) true
//      else if (this.size != that.size) false
//      else {
//        var isEq = true
//        var i = 0
//        val len = size
//        while (i < len && isEq) {
//          isEq = this.keyAt(i) == that.keyAt(i) && this.indexAt(i) == that.indexAt(i)
//          i += 1
//        }
//        isEq
//      }
//
//    case _ => false
//    }
  }

  @override
  int get hashCode => to[Vector].hashCode * 677;

//  implicit def cbf[K: Order: ClassTag]: CanBuildFrom[Index[_], (K, Int), Index<K>] =
//    new CanBuildFrom[Index[_], (K, Int), Index<K>] {
//      def apply(): mutable.Builder[(K, Int), Index<K>] = new IndexBuilder<K>
//      def apply(from: Index[_]): mutable.Builder[(K, Int), Index<K>] = apply()
//    }

  static IndexBuilder<K> newBuilder2() => new IndexBuilder();

  static Index<K> emptyIndex() =>
      new OrderedIndex<K>(new List<K>(0), new List<int>(0));

  static Index<K> fromKeys(Iterable<K> keys) => new Index(zipWithIndex(keys));

  factory Index.fromList(List<K> keys) =>
      new Index(keys, Array.range(0, keys.length));

  factory Index.fromPairs(Iterable<Tuple2<K, int>> pairs) {
    var un = pairs.unzip;
    return new Index.from(un.keys.toArray, un.indices.toArray);
  }

  factory Index.from(List<K> keys, List<int> indices) {
//    @tailrec
    bool isOrdered(int i) {
      if (i >= keys.length) {
        return true;
      } else if (keys(i - 1) > keys(i)) {
        return false;
      } else {
        return isOrdered(i + 1);
      }
    }

    if (isOrdered(1)) {
      return ordered(keys, indices);
    } else {
      return unordered(keys, indices);
    }
  }

  static OrderedIndex<K> ordered_(List<K> keys) =>
      ordered(keys, Array.range(0, keys.length));

  static OrderedIndex<K> ordered2_(List<K> keys, List<int> indices) =>
      new OrderedIndex(keys, indices);

  static Index<K> unordered_(List<K> keys) =>
      unordered(keys, Array.range(0, keys.length));

  static Index<K> unordered2_(List<K> keys, List<int> indices) {
    require(keys.length == indices.length);

    List<int> flip(List<int> xs) {
      var ys = new List<int>(xs.size);
      for (var i = 0; i < xs.size; i += 1) {
        ys[xs[i]] = i;
      }
      return ys;
    }

    List<A> shuffle(List<A> xs, List<int> order) {
      var ys = new List<A>(xs.length);
//      cfor(0)(_ < order.length, _ + 1) { i =>
//        ys(i) = xs(order(i))
//      }
      return ys;
    }

    var order0 = Array.range(0, keys.length).qsortedBy(keys(_));
    var indices0 = shuffle(indices, order0);
    var keys0 = shuffle(keys, order0);
    return new UnorderedIndex(keys0, indices0, flip(order0));
  }

  static int spanEnd(List<K> keys, K key, int i) {
    if (i < keys.length && keys(i) == key) {
      return spanEnd(keys, key, i + 1);
    } else {
      return i;
    }
  }

  static grouper.State group(Index<K> index, Grouper<K> grouper) {
    var keys = index.keys;
    var indices = index.indices;

    grouper.State loop(grouper.State s0, int start) {
      if (start < keys.length) {
        var end = spanEnd(keys, keys(start), start);
        var s1 = grouper.group(s0)(keys, indices, start, end);
        return loop(s1, end);
      } else {
        return s0;
      }
    }

    return loop(grouper.init, 0);
  }

  cogrouper.State cogroup(Index<K> lhs, Index<K> rhs, Cogrouper<K> cogrouper) {
    var lKeys = lhs.keys;
    var lIndices = lhs.indices;
    var rKeys = rhs.keys;
    var rIndices = rhs.indices;

    cogrouper.State loop(cogrouper.State s0, int lStart, int rStart) {
      if (lStart < lKeys.length && rStart < rKeys.length) {
        var lKey = lKeys(lStart);
        var rKey = rKeys(rStart);
        var ord = lKey.compare(rKey);
        var lEnd = (ord <= 0) ? spanEnd(lKeys, lKey, lStart + 1) : lStart;
        var rEnd = (ord >= 0) ? spanEnd(rKeys, rKey, rStart + 1) : rStart;
        var s1 = cogrouper.cogroup(s0)(
            lKeys, lIndices, lStart, lEnd, rKeys, rIndices, rStart, rEnd);
        return loop(s1, lEnd, rEnd);
      } else if (lStart < lKeys.length) {
        var lEnd = spanEnd(lKeys, lKeys(lStart), lStart + 1);
        var s1 = cogrouper.cogroup(s0)(
            lKeys, lIndices, lStart, lEnd, rKeys, rIndices, rStart, rStart);
        return loop(s1, lEnd, rStart);
      } else if (rStart < rKeys.length) {
        var rEnd = spanEnd(rKeys, rKeys(rStart), rStart + 1);
        var s1 = cogrouper.cogroup(s0)(
            lKeys, lIndices, lStart, lStart, rKeys, rIndices, rStart, rEnd);
        return loop(s1, lStart, rEnd);
      } else {
        return s0;
      }
    }

    loop(cogrouper.init, 0, 0);
  }
}

class _UnorderedIndex<K> extends Index<K> {
  List<K> _keys;
  List<int> _indices;
  List<int> _ord;

  _UnorderedIndex(this._keys, this._indices, this._ord);

  @override
  int size() => _keys.size;

  K keyAt(int i) => _keys(ord(i));
  int indexAt(int i) => _indices(ord(i));

  Iterator<Tuple2<K, int>> iterator2() => ord.iterator.map((i) {
        return Tuple2(_keys(i), _indices(i));
      });

  Unit foreach(f(Tuple<K, int> a)) {
    for (var i = 0; i < ord.length; i += 1) {
      val j = ord(i);
      f(keys(j), indices(j));
    }
  }

  Index<K> resetIndices() {
    var indices = new List<int>(keys.length);
    for (var i = 0, i < ord.lengthl i += 1) {
      indices[ord(i)] = i;
    }
    new UnorderedIndex(keys, indices, ord);
  }

  Tuple2<List<K>, List<int>> unzip() {
    var ks = new List<K>(ord.length);
    var ix = new List<int>(ord.length);
    for (var i = 0; i < ord.length; i + 1) {
      val j = ord(i);
      ks[i] = keys(j);
      ix[i] = indices(j);
    }
    return new Tuple2(ks, ix);
  }

  bool isOrdered() => false;

  Index<K> withIndices_(List<int> ix) => new UnorderedIndex(keys, ix, ord);
}

class OrderedIndex<K> extends Index<K> {
  List<K> _keys;
  List<int> _indices;

  OrderedIndex(this._keys, this._indices);

  @override
  int size() => _keys.size;
  K keyAt(int i) => _keys(i);
  int indexAt(int i) => _indices(i);
  Iterator<Tuple2<K, int>> iterator2() {
//    return Iterator.tabulate(keys.length) { i =>
//    (keys(i), indices(i))
//    }
  }

  Unit foreach(f(Tuple2<K, int> a)) {
    for (var i = 0; i < keys.length; i += 1) {
      f(keys(i), indices(i));
    }
  }

  Index<K> resetIndices() => new OrderedIndex(keys, Array.range(0, keys.size));

  Tuple2<Array<K>, Array<int>> unzip() =>
      new Tuple2(keys.clone(), indices.clone());

  bool isOrdered() => true;

  Index<K> withIndices_(List<int> ix) => new OrderedIndex(keys, ix);

  bool _isSequential = null;
  bool isSequential() {
    if (_isSequential != null) {
      return _isSequential;
    }
    _isSequential = true;
    var i = 0;
    while (i < indices.length && _isSequential) {
      _isSequential = _isSequential && indices(i) == i;
      i += 1;
    }
    return _isSequential;
  }
}

class IndexBuilder<K> {//extends mutable.Builder<Tuple2<K, int>, Index<K>> {
  var keys = mutable.ArrayBuilder.make();
  var indices = mutable.ArrayBuilder.make();

  var isOrdered = true;
  var isNonEmpty = false;
  K prev = _;

  /*this.type*/ add(K k, int i) {
    if (isOrdered && isNonEmpty && prev > k) {
      isOrdered = false;
    }

    keys += k;
    indices += i;

    prev = k;
    isNonEmpty = true;

    return this;
  }

  void operator +(Tuple2<K, int> elem) => add(elem.v1, elem.v2);

  Unit clear() {
    isOrdered = true;
    isNonEmpty = false;
    keys.clear();
    indices.clear();
  }

  Index<K> result() {
    if (isOrdered) {
      return Index.ordered(keys.result(), indices.result());
    } else {
      return Index.unordered(keys.result(), indices.result());
    }
  }
}

abstract class Grouper<K> {
  State type;

  State init();

  State group(State state, List<K> keys, List<int> indices, int start, int end);
}

/**
 * `Cogrouper` provides the abstraction used by `Index.cogroup` to work with
 * the result of a cogroup. Essentially, the cogroup will initiate some state,
 * then perform a cogroup on the 2 [[Index]]es, with each unique key resulting
 * in one call to `cogroup`. The reason the signature is a bit weird, with
 * the start/end offsets being passed in is to avoid copying and allocation
 * where possible. All implementations of [[Index]] can perform this operation
 * efficiently.
 */
abstract class Cogrouper<K> {
  State type;

  State init();

  State cogroup(State state, List<K> lKeys, List<int> lIdx, int lStart,
      int lEnd, List<K> rKeys, List<int> rIdx, int rStart, int rEnd);
}

// We cheat here and use a mutable state because an immutable one would just
// be too slow.
class State {
  mutable.ArrayBuilder<K> keys = mutable.ArrayBuilder.make();
  mutable.ArrayBuilder<int> lIndices = mutable.ArrayBuilder.make();
  mutable.ArrayBuilder<Int> rIndices = mutable.ArrayBuilder.make();

  add(K k, int l, int r) {
    keys += k;
    lIndices += l;
    rIndices += r;
  }

  Tuple3<List<K>, List<int>, List<int>> result() {
    return new Tuple3(keys.result(), lIndices.result(), rIndices.result());
  }
}

abstract class GenericJoin<K> extends Cogrouper<K> {
  init() => new State();

  static Skip() => Int.MinValue;
}
