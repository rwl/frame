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

abstract class ColumnTyper<A> {
  Column<A> cast(TypedColumn col);
}

//object ColumnTyper extends ColumnTyperInstances

//object ColumnTyperInstances {
abstract class ColumnTyper0 {
  /*implicit*/ defaultTyper() => new DefaultColumnTyper<A>();
}

abstract class ColumnTyper1 extends ColumnTyper0 {
  /*implicit*/ typeableTyper() => new TypeableColumnTyper<A>();
}

abstract class ColumnTyper2 extends ColumnTyper1 {
  /*implicit*/ ColumnTyper anyTyper;
  /*= new ColumnTyper[Any] {
    def cast(col: TypedColumn[_]): Column[Any] = col.column.asInstanceOf[Column[Any]]
  }*/
}

abstract class ColumnTyper3 extends ColumnTyper2 {
  /*implicit*/ ColumnTyper<int> int = new IntColumnTyper();
  /*implicit*/ ColumnTyper<long> long = new LongColumnTyper();
  /*implicit*/ ColumnTyper<float> float = new FloatColumnTyper();
  /*implicit*/ ColumnTyper<double> double = new DoubleColumnTyper();
  /*implicit*/ ColumnTyper<BigInt> bigInt = new BigIntTyper();
  /*implicit*/ ColumnTyper<BigDecimal> bigDecimal = new BigDecimalTyper();
  /*implicit*/ ColumnTyper<Rational> rational = new RationalTyper();
  /*implicit*/ ColumnTyper<Number> number = new NumberTyper();
}
//}

//trait ColumnTyperInstances extends ColumnTyperInstances.ColumnTyper3

class DefaultColumnTyper<A> extends ColumnTyper<A> {
  Column<A> cast(TypedColumn col) {
    /*if (classTag<A>.runtimeClass isAssignableFrom col.classTagA.runtimeClass) {
      col.column.asInstanceOf[Column<A>]
    } else {
      col.column.flatMap { _ => NM }
    }*/
  }
}

class TypeableColumnTyper<A> extends ColumnTyper<A> {
  Column<A> cast(TypedColumn col) {
    /*if (classTag<A>.runtimeClass isAssignableFrom col.classTagA.runtimeClass) {
      col.column.asInstanceOf[Column<A>]
    } else {
      col.column.flatMap { a => Cell.fromOption(a.cast<A>, NM) }
    }*/
  }
}
