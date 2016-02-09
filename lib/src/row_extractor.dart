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

abstract class RowExtractor<A, K, Sz extends Size> {
//  type P
  Option<P> prepare(Series<K, UntypedColumn> cols, List<K> keys);
  Cell<A> extract(int row, P p);

  RowExtractor<B, K, Sz> cellMap(Cell<B> f(Cell<A> c)) =>
      new MappedRowExtractor<A, B, K, Sz>(this, f);

  RowExtractor<B, K, Sz> map(B f(A a)) => cellMap((c) => c.map(f));

  RowExtractor<A, K, Sz> filter(bool p(A a)) => cellMap((c) => c.filter(p));

  RowExtractor<A, K, Sz> recover(PartialFunction<NonValue, A> pf) =>
      cellMap((c) => c.recover(pf));

  RowExtractor<A, K, Sz> recoverWith(PartialFunction<NonValue, Cell<A>> pf) =>
      cellMap((c) => c.recoverWith(pf));
}

class MappedRowExtractor<A, B, K, Sz extends Size>
    extends RowExtractor<B, K, Sz> {
  MappedRowExtractor(RowExtractor<A, K, Sz> e, Cell<B> f(Cell<A> c));

//  type P = e.P;

  Option<P> prepare(Series<K, UntypedColumn> cols, List<K> keys) =>
      e.prepare(cols, keys);

  Cell<B> extract(int row, P p) => f(e.extract(row, p));
}

//object RowExtractorLowPriorityImplicits {
abstract class RowExtractorLow0 {
  /*implicit*/ variableExtractorIsFixed /*<A, K, N extends Nat>*/ (
      /*implicit*/ RowExtractor<A, K, Variable> e);
  /*new RowExtractor[A, K, Fixed[N]] {
      type P = e.P
      def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option<P> =
        e.prepare(cols, keys)
      def extract(row: Int, p: P): Cell<A> =
        e.extract(row, p)
    }*/
}

abstract class RowExtractorLow1 extends RowExtractorLow0 {
  /*implicit*/ RowExtractor generic /*<A, B, K, S extends Size>*/ (
      /*implicit*/ Generic.Aux<A, B> generic, RowExtractor<B, K, S> extractor);
  /*new RowExtractor[A,K,S] {
      type P = extractor.P
      def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option<P> =
        extractor.prepare(cols, keys)
      def extract(row: Int, p: P): Cell<A> =
        extractor.extract(row, p) map (generic.from(_))
    }*/
}

abstract class RowExtractorLow2 extends RowExtractorLow1 {
  /*implicit*/ RowExtractor simpleRowExtractor /*[A: ColumnTyper, K]*/ () {}
  /*new RowExtractor[A, K, Fixed[Nat._1]] {
      type P = Column<A>
      def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option[Column<A>] =
        keys.headOption flatMap { idx => cols(idx).map(_.cast<A>).value }
      def extract(row: Int, col: Column<A>): Cell<A> =
        col(row)
    }*/
}

abstract class RowExtractorLow3 extends RowExtractorLow2 {
  /*implicit*/ RowExtractor hnilRowExtractor();
  /*= new RowExtractor[HNil, K, Fixed[_0]] {
    type P = Unit
    def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option[Unit] =
      if (keys.isEmpty) Some(()) else None
    def extract(row: Int, p: P): Cell[HNil] =
      Value(HNil)
  }*/
}
//}

abstract class RowExtractorLowPriorityImplicits
    extends RowExtractorLowPriorityImplicits.RowExtractorLow3 {}

abstract class RowExtractor extends RowExtractorLowPriorityImplicits {
  static /*implicit*/ RowExtractor hlistRowExtractor /*[H: ColumnTyper, T <: HList, K, N <: Nat]*/ (
      /*implicit*/
      RowExtractor<T, K, Fixed<N>> te) {}
  /*= new RowExtractor[H :: T, K, Fixed[Succ[N]]] {
      type P = (Column[H], te.P)

      def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option<P> = for {
        idx <- keys.headOption
        tail <- te.prepare(cols, keys.tail)
        col <- cols(idx).value
      } yield (col.cast[H] -> tail)

      def extract(row: Int, p: P): Cell[H :: T] = {
        val (col, tp) = p
        for {
          tail <- te.extract(row, tp)
          value <- col(row)
        } yield {
          value :: tail
        }
      }
    }*/

  static apply /*[A, C, S <: Size]*/ (/*implicit*/ RowExtractor<A, C, S> e) =>
      e;

  static RowExtractor collectionOf /*[CC[_], A, K]*/ (
      /*implicit*/ RowExtractor<A, K, Fixed<Nat._1>> e,
      CanBuildFrom<Nothing, Cell<A>, CC<Cell<A>>> cbf) {}
  /*= new RowExtractor[CC[Cell<A>], K, Variable] {
    type P = List[e.P]

    def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option<P> =
      keys.foldRight(Some(Nil): Option[List[e.P]]) { (key, acc) =>
        acc flatMap { ps => e.prepare(cols, key :: Nil).map(_ :: ps) }
      }

    def extract(row: Int, ps: List[e.P]): Cell[CC[Cell<A>]] = {
      val bldr = cbf()
      ps foreach { p =>
        bldr += e.extract(row, p)
      }
      Value(bldr.result())
    }
  }*/

  static RowExtractor denseCollectionOf /*[CC[_], A, K]*/ (
      /*implicit*/ RowExtractor<A, K, Fixed<Nat._1>> e,
      CanBuildFrom<Nothing, A, CC<A>> cbf) {}
  /*= new RowExtractor[CC<A>, K, Variable] {
    type P = List[e.P]

    def prepare(cols: Series[K, UntypedColumn], keys: List<K>): Option<P> =
      keys.foldRight(Some(Nil): Option[List[e.P]]) { (key, acc) =>
        acc flatMap { ps => e.prepare(cols, key :: Nil).map(_ :: ps) }
      }

    def extract(row: Int, ps: List[e.P]): Cell[CC<A>] = {
      val bldr = cbf()
      ps foreach { p =>
        e.extract(row, p) match {
          case Value(a) => bldr += a
          case (missing: NonValue) => return missing
        }
      }
      Value(bldr.result())
    }
  }*/
}
