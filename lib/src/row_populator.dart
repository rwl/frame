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

/// A trait used to generate frames from sets of rows.
///
/// The general use of an instance of this trait will be as follows:
///
/// ```
/// val pop = getSomeRowPopulator
/// val rows: List[(RowKey, RowData)]
/// val frame = pop.frame(rows.foldLeft(pop.init, (state, (row, data)) {
///   return pop.populate(state, row, data);
/// }));
/// ```
abstract class RowPopulator<A, Row, Col> {
//  type State
  State init();
  State populate(State state, Row row, A data);
  Frame<Row, Col> frame(State state);
}

abstract class RowPopulatorLowPriorityImplicits {
  static /*implicit*/ RowPopulator generic /*<A, B, Row, Col>*/ (
      /*implicit*/ Generic.Aux<A, B> generic, RowPopulator<B, Row, Col> pop) {}
  /*= new RowPopulator[A, Row, Col] {
      type State = pop.State
      def init: State = pop.init
      def populate(state: State, row: Row, data: A): State =
        pop.populate(state, row, generic.to(data))
      def frame(state: State): Frame[Row, Col] =
        pop.frame(state)
    }*/
}

//object RowPopulator extends RowPopulatorLowPriorityImplicits {
//implicit def HListRowPopulator[Row: Order: ClassTag, L <: HList](
//    implicit pop: HListColPopulator[L]) = new HListRowPopulator[Row, L](pop)

class HListRowPopulator<Row /*: Order: ClassTag*/, L extends HList>
    extends RowPopulator<L, Row, Int> {
  HListRowPopulator(HListColPopulator<L> pop);

//  type State = List[Row] :: pop.State

  State init(); // = Nil :: pop.init

  State populate(State state, Row row,
      L data); // => (row :: state.head) :: pop.populate(state.tail, data)

  Frame<Row, Int> frame(State state) {
    var cols = pop.columns(state.tail).toArray();
    var rowIndex = Index(state.head.reverse.toArray());
    var colIndex = Index(Array.range(0, cols.size));
    return ColOrientedFrame(rowIndex, colIndex, Column.dense(cols));
  }
}

abstract class HListColPopulator<L extends HList> {
//  type State <: HList
  State init();
  State populate(State state, L data);
  List<UntypedColumn> columns(State state);
}

//trait HListColPopulator0 {
/*implicit*/ class HNilColPopulator extends HListColPopulator<HNil> {
//    type State = HNil
  HNil init() => HNil;
  HNil populate(HNil u, HNil data) => HNil;
  List<UntypedColumn> columns(State state) => Nil;
}
//}

//object HListColPopulator extends HListColPopulator0 {
//  implicit def HConsColPopulator[H: ClassTag, T <: HList](implicit tail: HListColPopulator[T]) =
//    new HConsColPopulator(tail)

class HConsColPopulator<H, T extends HList>
    extends HListColPopulator<H /*:: T*/ > {
  HConsColPopulator(HListColPopulator<T> tail);

//    type State = List[H] :: tail.State

  Nil init(); // =>  :: tail.init

  State populate(State state,
      H /*:: T*/ data); // => (data.head :: state.head) :: tail.populate(state.tail, data.tail)

  List<UntypedColumn> columns(State state) {
    val col = TypedColumn(Column.dense(state.head.reverse.toArray));
//      col :: tail.columns(state.tail)
  }
}
//}
//}
