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

/// A [Cell] represents a single piece of data that may not be
/// available or meangingful in a given context.
///
/// Essentially, a [Cell] is similar to `Option`, except instead of
/// `None` we have 2 representations of [NonValue], the absence of
/// data: [NA] (Not Available) and [NM] (Not Meaningful).
abstract class Cell<A extends Comparable> implements Comparable<Cell<A>> {
  const Cell();

  int compareTo(Cell<A> other) {
    if (this is Value && other is Value) {
      return get.compareTo(other.get);
    } else if (this == NA && other == NA) {
      return 0;
    } else if (this == NM && other == NM) {
      return 0;
    } else if (this == NA) {
      return -1;
    } else if (other == NA) {
      return 1;
    } else if (this == NM) {
      return -1;
    } else if (other == NM) {
      return 1;
    } else {
      throw other;
    }
  }

  /// Returns true if this [Cell] is a value that is available and meaningful.
  bool get isValue;

  /// Returns true if this [Cell] is not a value that is available and
  /// meaningful.
  bool get isNonValue => !isValue;

  /// Return the [Cell]'s value.
  ///
  /// The [Cell]'s value must be both available and meaningful. Throws a
  /// [UnsupportedError] if the value is unavailable or not meaningful.
  A get get;

  /// Project this [Cell] to an `Option`. [Some] of the value or [None]
  /// for non values.
  Option<A> value();

  /// Returns the contents of the [Cell] as a `String`
  ///
  /// ```
  /// NA.valueString() == "NA"
  /// NA.valueString() == "NM"
  /// new Value(1.0).valueString() == "1.0"
  ///
  /// NA.toString() == "NA"
  /// NM.toString() == "NM"
  /// new Value(1.0).toString() == "Value(1.0)"
  /// ```
  String valueString();

  /// Returns the result of applying `f` to this [Cell]'s value.
  /// Otherwise, evaluates expression `na` if a value is not
  /// available, or evaluates expression `nm` if the value is
  /// not meaningful.
  dynamic fold(na(), nm(), f(A a));

  /// Returns the [Cell]'s value if it is available and meaningful,
  /// otherwise returns the result of evaluating expression `dflt`.
  dynamic getOrElse(dflt()) => isValue ? get : dflt();

  /// Returns a [Value] containing the result of applying `f` to
  /// this [Cell]'s value if it is available and meaningful.
  Cell map(dynamic f(A a)) => isValue ? new Value(f(get)) : this;

  /// Returns the result of applying `f` to this [Cell]'s value if
  /// it is available and meaningful.
  Cell flatMap(Cell f(A a)) => isValue ? f(get) : this;

  /// Flatten a nested [Cell] with type `Cell<Cell<B>>` into `Cell<B>`.
  Cell flatten() => isValue ? get : this;

  /// Returns this [Cell] unless it contains a value that is
  /// available and meaningful ''and'' applying the predicate `p` to
  /// that value returns false, then return [NA].
  Cell<A> filter(bool p(A a)) => (isNonValue || p(get)) ? this : NA;

  /// Returns this [Cell] unless it contains a value that is
  /// available and meaningful *and* applying the predicate `p` to
  /// that value returns true, then return [NA].
  Cell<A> filterNot(bool p(A a)) => (isNonValue || !p(get)) ? this : NA;

  /// If this cell is a [NonValue] and `pf` is defined for it, then this
  /// will return `Value(pf(this))`, otherwise it will return this cell as-is.
  Cell recover(PartialFunction pf) {
    if (this is NonValue) {
      var nonValue = this;
      if (pf.isDefinedOn(nonValue)) {
        return new Value(pf(nonValue));
      }
    }
    return this;
  }

  /// If this cell is a [NonValue] and `pf` is defined for it, then this
  /// will return `pf(this)`, otherwise it will return this cell as-is.
  Cell recoverWith(PartialFunction pf) {
    if (this == NonValue) {
      var nonValue = this;
      if (pf.isDefinedOn(nonValue)) {
        pf(nonValue);
      }
    }
    return this;
  }

  /// Returns true if this [Cell]'s value is available and
  /// meaningful *and* the predicate `p` returns true when applied
  /// to that value. Otherwise, returns false.
  bool exists(bool p(A a)) => isValue && p(get);

  /// Returns true if this [Cell]'s value is unavailable ([NA])
  /// *or* the predicate `p` returns true when applied to this
  /// [Cell]'s meaningful value.
  ///
  /// [NA] represents the vacuous case, so will result in
  /// true, but [NM] will result in false.
  bool forAll(bool p(A a));

  /// Apply the the given procedure `f` to the [Cell]'s value if it
  /// is available and meaningful. Otherwise, do nothing.
  void forEach(f(A a)) {
    if (isValue) {
      f(get);
    }
  }

  /// Returns a [Value] containing the result of appling `pf` to
  /// this [Cell]'s value if it is available and meaningful ''and''
  /// `pf` is defined for that value. Otherwise return [NA], unless
  /// this [Cell] is [NM].
  Cell collect(PartialFunction pf);

  /// Returns this [Cell] if its value is available and meaningful,
  /// otherwise return the result of evaluating `alternative`.
  Cell orElse(Cell alternative) => isValue ? this : alternative;

  /// Project this [Cell] to an `Option`. [Some] of the value or [None]
  /// for non values
  Option<A> toOption() => value();

  /// Returns a singleton list containing the [Cell]'s value, or
  /// the empty list if the [Cell]'s value is unavailable or not
  /// meaningful.
  List<A> toList() => isValue ? [get] : [];

  /// If both `this` and `that` are values, then this returns a value derived
  /// by applying `f` to the values of them. Otherwise, if either `this` or
  /// `that` is `NA`, then `NA` is returned, otherwise `NM` is returned.
  Cell zipMap(Cell that, f(A a, b)) {
    if (this is Value && that is Value) {
      return new Value(f(this.get, that.get));
    } else if (this == NA || that == NA) {
      return NA;
    } else {
      return NM;
    }
  }

  // TODO: there are currently issues where we get comparison between
  // Value(NA) and NA and this should be true the current tweaks to
  // equality are just holdovers until we figure out some more details
  // on the implementation of non values.

  factory Cell.fromOption(Option<A> opt, [NonValue nonValue]) {
    if (nonValue == null) {
      nonValue = NA;
    }
    if (opt is Some) {
      return new Value(opt.get());
    } else if (opt is None) {
      return nonValue;
    }
  }
}

/// The supertype of non values, [NA] (''Not Available'') and
/// [NM] (''Not Meaningful'')
abstract class NonValue extends Cell {
  const NonValue();

  bool get isValue => false;

  value() => None;

  @override
  bool operator ==(that) {
    if (that == null) {
      return false;
    } else if (that is NonValue) {
      if (identical(that, NA)) {
        return identical(this, NA);
      } else if (identical(that, NM)) {
        return identical(this, NM);
      } else {
        return false;
      }
    } else if (that is Value) {
      if (identical(that.get, NA)) {
        return identical(this, NA);
      } else if (identical(that.get, NM)) {
        return identical(this, NM);
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}

const NotAvailable NA = const NotAvailable();

/// A value is *Not Available (NA)*
///
/// This represents the absence of any data.
class NotAvailable extends NonValue {
  const NotAvailable() : super();

  dynamic get get => throw new UnsupportedError("NA.get");

  String valueString() => "NA";

  dynamic fold(na(), nm(), f(a)) => na();

  bool forAll(bool p(a)) => true;

  Cell collect(PartialFunction pf) {
    if (pf.isDefinedOn(get)) {
      return new Value(pf(get));
    } else {
      return NA;
    }
  }

  String toString() => "NA";
}

const NotMeaningful NM = const NotMeaningful();

/// The value is *Not Meaningful (NM)*.
///
/// This indicates that data exists, but that it is not meaningful.
/// For instance, if we divide by 0, then the result is not
/// meaningful, but we wouldn't necessarily say that data is
/// unavailable.
class NotMeaningful extends NonValue {
  const NotMeaningful() : super();

  dynamic get get => throw new UnsupportedError("NM.get");

  String valueString() => "NM";

  dynamic fold(na(), nm(), f(A a)) => nm();

  bool forAll(bool p(A a)) => false;

  Cell collect(PartialFunction pf) => NM;

  String toString() => "NM";
}

/// A value that is meaningful.
class Value<A> extends Cell<A> {
  final A get;

  Value(this.get);

  Option value() => new Some(get);

  String valueString() => get.toString();

  bool get isValue => (get == NA || get == NM) ? false : true;

  dynamic fold(na(), nm(), f(A a)) => f(get);

  bool forAll(bool p(A a)) => p(this.get);

  Cell collect(PartialFunction pf) {
    if (pf.isDefinedOn(get)) {
      return new Value(pf(get));
    } else {
      return NA;
    }
  }

  String toString() => "Value($get)";

  @override
  bool operator ==(that) {
    if (that == null) {
      return false;
    } else if (that is Cell) {
      if (identical(that, NA)) {
        return identical(this, NA);
      } else if (identical(that, NM)) {
        return identical(this, NM);
      } else if (identical(that.get, NA)) {
        return identical(this, NA);
      } else if (identical(that.get, NM)) {
        return identical(this, NM);
      } else if (identical(get, NA)) {
        return identical(that, NA);
      } else if (identical(get, NM)) {
        return identical(that, NM);
      } else {
        return get == that.get;
      }
    } else {
      return false;
    }
  }
}
