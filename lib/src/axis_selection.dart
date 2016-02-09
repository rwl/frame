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

typedef Cell<A> Extractor(int i);

abstract class AxisSelection<K, A> {
  Extractor extractorFor(Series<K, UntypedColumn> cols);

  void forEach(Index index, Series<K, UntypedColumn> cols,
      dynamic f(i, int j, Cell<A> c)) {
    var get = extractorFor(cols);
    index.forEach((key, row) {
      f(key, row, get(row));
    });
  }

  AxisSelection<K, B> cellMap(Cell f(Cell<A> c));

  AxisSelection<K, B> map(dynamic f(A a));

  AxisSelection<K, A> filter(bool f(A a));

  AxisSelection<K, A> recoverWith(PartialFunction<NonValue, Cell<A>> pf);

  AxisSelection<K, A> recover(PartialFunction<NonValue, A> pf);
}

abstract class AxisSelectionLike<K, A,
        This /*<K, A>*/ /*<: AxisSelectionLike<K, A, This>*/ >
    extends AxisSelection<K, A> {
  This<K, B> cellMap(Cell f(Cell<A> cell));

  This<K, A> orElse(This<K, A> that);

  This<K, B> flatMap(This<K, dynamic> f(A a));

  This<K, dynamic> zipWith(This<K, dynamic> that, dynamic f(A a, b));

  This<K, dynamic> map(dynamic f(A a)) => cellMap((c) => c.map(f));

  This<K, A> filter(bool p(A a)) => cellMap((c) => c.filter(p));

  This<K, A> recoverWith(PartialFunction<NonValue, Cell<A>> pf) =>
      cellMap((c) => c.recoverWith(pf));

  This<K, A> recover(PartialFunction<NonValue, A> pf) =>
      cellMap((c) => c.recover(pf));

  This<K, Tuple<A, dynamic>> zip(This<K, B> that) =>
      zipWith(that, (a, b) => new Tuple2(a, b));
}

abstract class AxisSelectionCompanion<
    Sel /*<K, A> <: AxisSelectionLike<K, A, Sel>*/ > {
  all() => new All<K, Rec<K>>(new RowExtractor<Rec<K>, K, Variable>());

  unsized(Seq<K> cols) =>
      new Pick(cols.toList, new RowExtractor<Rec<K>, K, Variable>());

  Pick<K, Fixed<N>, Rec<K>> sized /*<K, N extends Nat>*/ (
          Sized<List<K>, N> s) =>
      new Pick(s.unsized, new RowExtractor<Rec<K>, K, Fixed<N>>());

  Pick<K, Fixed /*[_1]*/, Rec<K>> apply(K c0) => sized(Sized[List](c0));

  Pick<K, Fixed /*[_2]*/, Rec<K>> apply(K c0, K c1) =>
      sized(Sized[List](c0, c1));

  Pick<K, Fixed /*[_3]*/, Rec<K>> apply(K c0, K c1, K c2) =>
      sized(Sized[List](c0, c1, c2));

  Pick<K, Fixed /*[_4]*/, Rec<K>> apply(K c0, K c1, K c2, K c3) =>
      sized(Sized[List](c0, c1, c2, c3));

  Pick<K, Fixed /*[_5]*/, Rec<K>> apply(K c0, K c1, K c2, K c3, K c4) =>
      sized(Sized[List](c0, c1, c2, c3, c4));

  /*def apply<K>(c0: K, c1: K, c2: K, c3: K, c4: K, c5: K): Pick[K, Fixed[_6], Rec<K>] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5))

  def apply<K>(c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K): Pick[K, Fixed[_7], Rec<K>] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6))

  def apply<K>(c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K): Pick[K, Fixed[_8], Rec<K>] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7))

  def apply<K>(c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K, c8: K): Pick[K, Fixed[_9], Rec<K>] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7, c8))

  def apply<K>(c0: K, c1: K, c2: K, c3: K, c4: K, c5: K, c6: K, c7: K, c8: K, c9: K): Pick[K, Fixed[_10], Rec<K>] =
    sized(Sized[List](c0, c1, c2, c3, c4, c5, c6, c7, c8, c9))*/
}

//type All<K, A> <: AllAxisSelection<K, A> with Sel<K, A>
//def All: AllCompanion
class All<K, A> {}

abstract class AllCompanion {
  All<K, A> apply(RowExtractor<A, K, Variable> extractor);
}

//type Pick[K, S <: Size, A] <: PickAxisSelection[K, S, A] with Sel<K, A>
//def Pick: PickCompanion
class Pick<K, S extends Size, A> {}

abstract class PickCompanion {
  Pick<K, S, A> apply(List<K> keys, RowExtractor<A, K, S> extractor);
}

//type Wrapped<K, A> <: WrappedAxisSelection<K, A> with Sel<K, A>
//def Wrapped: WrappedCompanion
class Wrapped<K, A> {}

abstract class WrappedCompanion {
  Wrapped<K, A> apply(AxisSelection<K, A> sel);
}

abstract class Bridge<K, A> extends AxisSelectionLike<K, A, Sel> {
//  self: Sel<K, A> =>
  Sel<K, A> orElse(Sel<K, A> that) => Wrapped(ops.OrElse(this, that));
  Sel<K, B> flatMap(Sel<K, B> f(A a)) => Wrapped(ops.Bind(this, f));
  Sel<K, C> zipWith(Sel<K, B> that, C f(A a, B b)) =>
      Wrapped(ops.Zipped(this, that, (a, b) => a.zipMap(b)(f)));
}

abstract class SizedAxisSelection<K, Sz extends Size, A> extends Bridge<K, A> {
//  self: Sel<K, A> =>
  RowExtractor<A, K, Sz> extractor;

  List<K> getOrElse(List<K> all()) => fold(all, (keys) => keys);

  Extractor extractorFor(Series<K, UntypedColumn> cols) {
    var colKeys = getOrElse(cols.index.keys.toList);
    var ret = extractor.prepare(cols, colKeys);
    if (ret is Some) {
      return (row) => extractor.extract(row, ret.get);
    } else if (ret is None) {
      return (row) => NA;
    }
  }

  @override
  void foreach(
      Index index, Series<K, UntypedColumn> cols, U f(I i, Int j, Cell<A> c)) {
    var colKeys = getOrElse(cols.index.keys.toList);
    for (var p in extractor.prepare(cols, colKeys)) {
      index.foreach((key, row) {
        return f(key, row, extractor.extract(row, p));
      });
    }
  }

  B fold(B all(), B f(List<K> l));

  Sel<K, B> as_(/*implicit*/ RowExtractor<B, K, Sz> extractor0);
}

abstract class AllAxisSelection<K, A>
    extends SizedAxisSelection<K, Variable, A> {
//  self: Sel<K, A> =>
  RowExtractor<A, K, Variable> extractor;

  B fold(B all(), B f(List<K> l)) => all();

  All<K, B> cellMap(Cell<B> f(Cell<A> c)) => new All(extractor.cellMap(f));

  Sel<K, B> as_(/*implicit*/ RowExtractor<B, K, Variable> extractor0) =>
      new All(extractor0);

  Sel<K, List<Cell<B>>> asListOf(
          /*implicit*/ RowExtractor<B, K, Fixed<Nat._1>> extractor0) =>
      as(RowExtractor.collectionOf);

  Sel<K, Vector<Cell<B>>> asVectorOf(
          /*implicit*/ RowExtractor<B, K, Fixed<Nat._1>> extractor0) =>
      as(RowExtractor.collectionOf);

  Sel<K, Array<B>> asArrayOf(
          /*implicit*/ RowExtractor<B, K, Fixed<Nat._1>> extractor0) =>
      as(RowExtractor.denseCollectionOf);
}

abstract class PickAxisSelection<K, S extends Size, A>
    extends SizedAxisSelection<K, S, A> {
//  self: Sel<K, A> =>
  List<K> keys;
  RowExtractor<A, K, S> extractor;

  B fold(B all(), B f(List<K> l)) => f(keys);

  Pick<K, S, B> cellMap(Cell<B> f(Cell<A> c)) =>
      new Pick(keys, extractor.cellMap(f));

  Pick<K, Variable, A> variable() => this as Pick<K, Variable, A>;

  Sel<K, B> as_(/*implicit*/ RowExtractor<B, K, S> extractor0) =>
      Pick(keys, extractor0);

  Sel<K, List<Cell<B>>> asListOf(
          /*implicit*/ RowExtractor<B, K, Fixed<Nat._1>> extractor0) =>
      variable.as(RowExtractor.collectionOf);

  Sel<K, Vector<Cell<B>>> asVectorOf(
          /*implicit*/ RowExtractor<B, K, Fixed<Nat._1>> extractor0) =>
      variable.as(RowExtractor.collectionOf);

  Sel<K, Array<B>> asArrayOf(
          /*implicit*/ RowExtractor<B, K, Fixed<Nat._1>> extractor0) =>
      variable.as(RowExtractor.denseCollectionOf);
}

abstract class WrappedAxisSelection<K, A> extends Bridge<K, A> {
//  self: Sel<K, A> =>
  AxisSelection<K, A> sel();

  Sel<K, B> cellMap(Cell<B> f(Cell<A> a)) => Wrapped(sel.cellMap(f));

  Extractor extractorFor(Series<K, UntypedColumn> cols) =>
      sel.extractorFor(cols);

  @override
  void foreach(Index index, Series<K, UntypedColumn> cols,
          U f(I i, Int i, Cell<A> c)) =>
      sel.foreach(index, cols, f);
}

//object ops {
abstract class Op<K, A> extends AxisSelection<K, A> {
  AxisSelection<K, B> cellMap(Cell<B> f(Cell<A> c));

  AxisSelection<K, B> map(B f(A a)) => cellMap((c) => c.map(f));

  AxisSelection<K, A> filter(bool p(A a)) => cellMap((c) => c.filter(p));

  AxisSelection<K, A> recoverWith(PartialFunction<NonValue, Cell<A>> pf) =>
      cellMap((c) => c.recoverWith(pf));

  AxisSelection<K, A> recover(PartialFunction<NonValue, A> pf) =>
      cellMap((c) => c.recover(pf));
}

class Zipped<K, A, B, C> extends Op<K, C> {
  Zipped(AxisSelection<K, A> fst, AxisSelection<K, B> snd,
      Cell<C> combine(Cell<A> a, Cell<B> f()));

  Extractor extractorFor(Series<K, UntypedColumn> cols) {
    var get1 = fst.extractorFor(cols);
    var get2 = snd.extractorFor(cols);

    return (row) => combine(get1(row), get2(row));
  }

  AxisSelection<K, D> cellMap(Cell<D> f(Cell<C> c)) =>
      new Zipped<K, A, B, D>(fst, snd, (a, b) => f(combine(a, b)));
}

AxisSelection<K, A> OrElse(AxisSelection<K, A> fst, AxisSelection<K, A> snd) =>
    new Zipped<K, A, A, A>(fst, snd, (a, b) => a.orElse(b));

class Bind<K, A, B> extends Op<K, B> {
  Bind(AxisSelection<K, A> sel, AxisSelection<K, B> k(A a));

  Extractor extractorFor(Series<K, UntypedColumn> cols) {
    var get = sel.extractorFor(cols);

    return (row) => get(row).map(k).flatMap(_.extractorFor(cols)(row));
  }

  AxisSelection<K, C> cellMap(Cell<C> f(Cell<B> c)) =>
      new Bind<K, A, C>(sel, (a) => k(a).cellMap(f));
}
//}
