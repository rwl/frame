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

/** A [[Cell]] represents a single piece of data that may not be
  * available or meangingful in a given context.
  *
  * Essentially, a [[Cell]] is similar to `Option`, except instead of
  * `None` we have 2 representations of [[NonValue]], the absence of
  * data: [[NA]] (Not Available) and [[NM]] (Not Meaningful).
  *
  * @tparam A the value type contain in the cell
  * @see [[Value]] [[[NonValue]]] [[NA]] [[NM]]
  */
abstract class Cell<A> {
  /** Returns true if this [[Cell]] is a value that is available and meaningful.
    *
    * @return true if this [[Cell]] is a value that is available and meaningful.
    * @see [[Value]]
    */
  bool isValue();

  /** Returns true if this [[Cell]] is not a value that is available and meaningful.
    *
    * @return true if this [[Cell]] is not a value that is available and meaningful.
    * @see [[NonValue]]
    * @see [[NA]]
    * @see [[NM]]
    */
  bool isNonValue() => !isValue;

  /** Return the [[Cell]]'s value.
    *
    * @note The [[Cell]]'s value must be both available and meaningful.
    * @throws NoSuchElementException if the value is unavailable or not meaningful.
    */
  A get();

  /** Project this [[Cell]] to an `Option`
    *
    * @return Some of the value or None for non values
    */
  Option<A> value();

  /** Returns the contents of the [[Cell]] as a `String`
    *
    * @note {{{
    * NA.valueString == "NA"
    * NA.valueString == "NM"
    * Value(1D).valueString == "1.0"
    *
    * NA.toString == "NA"
    * NM.toString == "NM"
    * Value(1D).toString == "Value(1.0)"
    * }}}
    *
    * @return the content of the [[Cell]] as a `String`.
    */
  String valueString();

  /** Returns the result of applying `f` to this [[Cell]]'s value.
    * Otherwise, evaluates expression `na` if a value is not
    * available, or evaluates expression `nm` if the value is
    * not meaningful.
    *
    * @param  na  the expression to evaluate if no value is available.
    * @param  nm  the expression to evaluate if the value in not meaningful.
    * @param  f   the function to apply to a value that is available and meaningful.
    */
  B fold(na(), nm(), f(A a)) {
    if (this == NA) {
      return na();
    } else if (this == NM) {
      return nm();
    } else {
      var a = Value(this);
      return f(a);
    }
  }

  /** Returns the [[Cell]]'s value if it is available and meaningful,
    * otherwise returns the result of evaluating expression `default`.
    *
    * @param  default  the default expression.
    * @see [[get]]
    * @see [[orElse]]
    */
  getOrElse(dflt) => isValue() ? get() : dflt;

  /** Returns a [[Value]] containing the result of applying `f` to
    * this [[Cell]]'s value if it is available and meaningful.
    *
    * @param  f  the function to apply to a value that is available and meaningful.
    * @see [[flatMap]]
    * @see [[foreach]]
    */
  Cell<B> map(f(A a)) => isValue ? new Value(f(get())) : asInstanceOf[Cell];

  /** Returns the result of applying `f` to this [[Cell]]'s value if
    * it is available and meaningful.
    *
    * @param  f  the function to apply to a value that is available and meaningful.
    * @see [[map]]
    * @see [[foreach]]
    */
  Cell flatMap(Cell f(A a)) => isValue ? f(get()) : asInstanceOf[Cell];

  /** Flatten a nested [[Cell]] with type `Cell[Cell<B>]` into `Cell<B>`.
    *
    * @note there must be implicit evident that `A` is a subtype of `Cell<B>`.
    */
  Cell<B> flatten(/*A <:< Cell<B> ev*/) =>
      isValue ? ev(get()) : asInstanceOf[Cell];

  /** Returns this [[Cell]] unless it contains a value that is
    * available and meaningful ''and'' applying the predicate `p` to
    * that value returns false, then return [[NA]].
    *
    * @param  p  the predicate used to test a value that is available and meaningful.
    * @see [[filterNot]]
    * @see [[collect]]
    */
  Cell<A> filter(bool p(A a)) => (isNonValue || p(get())) ? this : NA;

  /** Returns this [[Cell]] unless it contains a value that is
    * available and meaningful ''and'' applying the predicate `p` to
    * that value returns true, then return [[NA]].
    *
    * @param  p  the predicate to test a value that is available and meaningful.
    * @see [[filter]]
    * @see [[collect]]
    */
  Cell<A> filterNot(bool p(A a)) => (isNonValue || !p(get())) ? this : NA;

  /** If this cell is a [[NonValue]] and `pf` is defined for it, then this
    * will return `Value(pf(this))`, otherwise it will return this cell as-is.
    *
    * @param pf the partial function to map the non-value.
    * @see [[recoverWith]]
    */
  Cell recover(PartialFunction<NonValue, A0> pf) {
    if (this is NonValue) {
      var nonValue = this;
      if (pf.isDefinedAt(nonValue)) {
        return new Value(pf(nonValue));
      }
    } else {
      var value = this;
      return false;
    }
  }

  /** If this cell is a [[NonValue]] and `pf` is defined for it, then this
    * will return `pf(this)`, otherwise it will return this cell as-is.
    *
    * @param pf the partial function to map the non-value.
    * @see [[recover]]
    */
  Cell recoverWith(PartialFunction<NonValue, Cell> pf) {
    if (this == NonValue) {
      var nonValue = this;
      if (pf.isDefinedAt(nonValue)) {
        pf(nonValue);
      }
    } else {
      var value = this;
      retuen value;
    }
  }

  /** Returns true if this [[Cell]]'s value is available and
    * meaningful ''and'' the predicate `p` returns true when applied
    * to that value. Otherwise, returns false.
    *
    * @param  p  the predicate to test a value that is available and meaningful.
    * @see [[forall]]
    */
  bool exists(bool p(A a)) => isValue() && p(get());

  /** Returns true if this [[Cell]]'s value is unavailable ([[NA]])
    *''or'' the predicate `p` returns true when applied to this
    * [[Cell]]'s meaningful value.
    *
    * @note [[NA]] represents the vacuous case, so will result in
    * true, but [[NM]] will result in false.
    *
    * @param  p  the predicate to test a value that is available and meaningful.
    * @see [[exists]]
    */
  bool forall(bool p(A a)) {
    if (this == NA) {
      return true;
    } else if (this == NM) {
      return false;
    } else {
      var a = new Value(a);
      return p(a);
    }
  }

  /** Apply the the given procedure `f` to the [[Cell]]'s value if it
    * is available and meaningful. Otherwise, do nothing.
    *
    * @param  f  the procedure to apply to a value that is available and meaningful.
    * @see [[map]]
    * @see [[flatMap]]
    */
  void foreach(f(A a)) {
    if (isValue()) {
      f(get());
    }
  }

  /** Returns a [[Value]] containing the result of appling `pf` to
    * this [[Cell]]'s value if it is available and meaningful ''and''
    * `pf` is defined for that value. Otherwise return [[NA]], unless
    * this [[Cell]] is [[NM]].
    *
    * @param  pf  the partial function to apply to a value that is available and meaningful.
    * @see [[filter]]
    * @see [[filterNot]]
    */
  Cell collect(PartialFunction<A, B> pf) {
    if (this == NM) {
      return NM;
    } else if (pf.isDefinedAt(a)) {
//      var a = new Value(a);
      return new Value(pf(a));
    } else {
      return NA;
    }
  }

  /** Returns this [[Cell]] if its value is available and meaningful,
    * otherwise return the result of evaluating `alternative`.
    *
    * @param  alternative  the alternative expression
    * @see [[getOrElse]]
    */
  Cell orElse(Cell<B> alternative) => isValue ? this : alternative;

  /** Project this [[Cell]] to an `Option`
    *
    * @return Some of the value or None for non values
    */
  Option<A> toOption() => value();

  /** Returns a singleton list containing the [[Cell]]'s value, or
    * the empty list if the [[Cell]]'s value is unavailable or not
    * meaningful.
    */
  List<A> toList() => isValue ? [get()] : [];

  /** If both `this` and `that` are values, then this returns a value derived
    * by applying `f` to the values of them. Otherwise, if either `this` or
    * `that` is `NA`, then `NA` is returned, otherwise `NM` is returned.
    */
  Cell zipMap(Cell<B> that, f(A a, B b)) {
//    (this, that) match {
//    case (Value(a), Value(b)) => Value(f(a, b))
//    case (NA, _) | (_, NA) => NA
//    case _ => NM
  }

// TODO: there are currently issues where we get comparison between Value(NA) and NA and this should be true
// the current tweaks to equality are just holdovers until we figure out some more details on the implementation
// of non values.
//object Cell extends CellInstances {
  static Cell<A> value2(A x) => Value(x);
  static Cell<A> notAvailable() => NA;
  static Cell<A> notMeaningful() => NM;

  static Cell<A> fromOption(Option<A> opt, [NonValue nonValue = NA]) {
//      opt match {
//      case Some(a) => Value(a)
//      case None => nonValue
//    }
  }
}

/** The supertype of non values, [[NA]] (''Not Available'') and
  * [[NM]] (''Not Meaningful'')
  *
  * @see [[Cell]] [[NA]] [[NM]]
  */
abstract class NonValue extends Cell {
  bool isValue() => false;
  value() => None;

  @override
  bool operator ==(that) {
//    that match {
//    case Value(thatValue) => this == thatValue
//    case _ => super.equals(that)
  }
}

/** A value is ''Not Available (NA)''
  *
  * This represents the absence of any data.
  *
  * @see [[Cell]] [[NonValue]] [[NM]] [[Value]]
  */
class _NA extends NonValue {
  get() => throw new NoSuchElementException("NA.get");
  String valueString() => "NA";
}

final _NA NA = new _NA();

/** The value is ''Not Meaningful (NM)''.
  *
  * This indicates that data exists, but that it is not meaningful.
  * For instance, if we divide by 0, then the result is not
  * meaningful, but we wouldn't necessarily say that data is
  * unavailable.
  *
  * @see [[Cell]] [[NonValue]] [[NA]] [[Value]]
  */
class _NM extends NonValue {
  get() => throw new NoSuchElementException("NM.get");
  String valueString() => "NM";
}

final _NM NM = new _NM();

/** A value that is meaningful.
  *
  * @tparam A the type of the value contained
  * @see [[Cell]] [[NonValue]] [[NA]] [[NM]]
  */
class Value<A> extends Cell<A> {
  A _get;
  Value(this._get);

  value() => Some(_get);
  String valueString() => _get.toString();

  bool isValue() => (_get == NA || _get == NM) ? false : true;

  @override
  bool operator ==(that) {
//    that match {
//    case Value(Value(NA)) => get == NA
//    case Value(Value(NM)) => get == NM
//    case Value(thatValue) => thatValue == get
//    case v @ NA => get == NA
//    case v @ NM => get == NM
//    case _ => false
  }
}

class _CellInstances {
//  trait CellInstances0 {
//    implicit def cellEq[A: Eq]: Eq[Cell<A>] = new CellEq<A>
//  }
}

final _CellInstances CellInstances = new _CellInstances();

abstract class CellInstances extends CellInstances0 {
//  implicit def cellOrder[A: Order]: Order[Cell<A>] = new CellOrder<A>
//  implicit def cellMonoid[A: Semigroup]: Monoid[Cell<A>] = new CellMonoid<A>
}

//private final class CellEq[A: Eq] extends Eq[Cell<A>] {
//  import spire.syntax.eq._
//
//  def eqv(x: Cell<A>, y: Cell<A>): Boolean = (x, y) match {
//    case (Value(x0), Value(y0)) => x0 === y0
//    case (NA, NA) | (NM, NM) => true
//    case _ => false
//  }
//
//  /*def eqv[X >: A: Eq, Y >: A: Eq](x: Cell[X], y: Cell[Y]): Boolean = (x, y) match {
//    case (Value(NA), NA) | (Value(NM), NM) | (NA, Value(NA)) | (NM, Value(NM)) => true
//    case _ => false
//  }*/
//}

//private final class CellOrder[A: Order] extends Order[Cell<A>] {
//  def compare(x: Cell<A>, y: Cell<A>): Int = (x, y) match {
//    case (Value(x0), Value(y0)) => x0 compare y0
//    case (NA, NA) | (NM, NM) => 0
//    case (NA, _) => -1
//    case (_, NA) => 1
//    case (NM, _) => -1
//    case (_, NM) => 1
//  }
//}

//private final class CellMonoid[A: Semigroup] extends Monoid[Cell<A>] {
//  def id: Cell<A> = NA
//  def op(x: Cell<A>, y: Cell<A>): Cell<A> = (x, y) match {
//    case (NM, _) => NM
//    case (_, NM) => NM
//    case (Value(a), Value(b)) => Value(a |+| b)
//    case (Value(_), _) => x
//    case (_, Value(_)) => y
//    case (NA, NA) => NA
//  }
//}
