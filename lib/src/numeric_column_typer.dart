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

// Currently support the following number types:
//   - Int
//   - Long
//   - Float
//   - Double
//   - BigInt
//   - BigDecimal
//   - Rational
//   - Number
//
// TODO: Support Algebraic and Real.

abstract class NumericColumnTyper<A> extends ColumnTyper<A> {
  Option<A> castValue(x);
  Option<Column<A>> castColumn(TypedColumn column);

  Column<A> cast(TypedColumn col) {
    Column column = col.column;
    castColumn(col).getOrElse(column.flatMap((x) {
      return Cell.fromOption(castValue(x), NM);
    }));
  }
}

abstract class Classes {
  var Byte = lang.Byte.TYPE;
  var Short = lang.Short.TYPE;
  var Int = Integer.TYPE;
  var Long = lang.Long.TYPE;
  var Float = lang.Float.TYPE;
  var Double = lang.Double.TYPE;
  var BigInt = classOf[math.BigInt];
  var BigInteger = classOf[math.BigInteger];
  var BigDecimal = classOf[math.BigDecimal];
  var JavaBigDecimal = classOf[math.BigDecimal];
  var Rational = classOf[Rational];
  var Number = classOf[Number];
  var String = classOf[String];
}

abstract class NumericColumnTyper {
  var scalaNumberClass = classTag[ScalaNumericAnyConversions].runtimeClass;

  bool isScalaNumber(Klass runtimeClass) =>
      scalaNumberClass.isAssignableFrom(runtimeClass);

  A foldValue(
      x,
      A primInt(Long a),
      A primFloat(Double a),
      A bigInt(BigInt a),
      A bigFloat(BigDecimal a),
      A rational(Rational a),
      A string(String a),
      A z()) {
    /*x match {
      case (x: Byte) => primInt(x.asInstanceOf<byte>.toLong)
      case (x: Short) => primInt(x.toLong)
      case (x: Int) => primInt(x.toLong)
      case (x: Long) => primInt(x)
      case (x: Float) => primFloat(x.toDouble)
      case (x: Double) => primFloat(x)
      case (x: BigInt) => bigInt(x)
      case (x: java.math.BigInteger) => bigInt(BigInt(x))
      case (x: BigDecimal) => bigFloat(x)
      case (x: java.math.BigDecimal) => bigFloat(BigDecimal(x))
      case (x: Rational) => rational(x)
      case (x: Number) =>
        if (x.isWhole) {
          if (x.withinLong) primInt(x.toLong)
          else bigInt(x.toBigInt)
        } else {
          if (x.isExact) rational(x.toRational)
          else bigFloat(x.toBigDecimal)
        }
      case _ => z
    }*/
  }

  A foldColumn(
      TypedColumn col,
      A primInt(Column<Long> a),
      A primFloat(Column<Double> a),
      A bigInt(Column<BigInt> a),
      A bigFloat(Column<BigDecimal> a),
      A rational(Column<Rational> a),
      A string(Column<String> a),
      A z()) {
    /*var column = col.column
    var runtimeClass = col.classTagA.runtimeClass
    runtimeClass match {
      case Classes.Byte => primInt(column.asInstanceOf[Column<byte>] map (_.toLong))
      case Classes.Short => primInt(column.asInstanceOf[Column<Short>] map (_.toLong))
      case Classes.Int => primInt(column.asInstanceOf[Column<int>] map (_.toLong))
      case Classes.Long => primInt(column.asInstanceOf[Column<long>])
      case Classes.Float => primFloat(column.asInstanceOf[Column[Float]] map (_.toDouble))
      case Classes.Double => primFloat(column.asInstanceOf[Column[Double]])
      case Classes.BigInt => bigInt(column.asInstanceOf[Column[BigInt]])
      case Classes.BigInteger => bigInt(column.asInstanceOf[Column[java.math.BigInteger]] map (BigInt(_)))
      case Classes.BigDecimal => bigFloat(column.asInstanceOf[Column[BigDecimal]])
      case Classes.JavaBigDecimal => bigFloat(column.asInstanceOf[Column[java.math.BigDecimal]] map (BigDecimal(_)))
      case cls if Classes.Rational isAssignableFrom cls => rational(column.asInstanceOf[Column[Rational]])
      case _ => z
    }*/
  }
}
/*
class IntColumnTyper extends ColumnTyper<int> {
  Cell<int> safeToInt(A n, /*implicit*/ ScalaNumericAnyConversions f(A a)) {
    var m = f(n).toInt();
    return (n == m) ? Value(m) : NM;
  }

  private var castValue: Any => Cell<int> = foldValue(_)(
    n => if (n >= Int.MinValue && n <= Int.MaxValue) Value(n.toInt) else NM,
    safeToInt(_),
    n => if (n >= Int.MinValue && n <= Int.MaxValue) Value(n.toInt) else NM,
    safeToInt(_),
    safeToInt(_),
    n => Try(BigDecimal(n).toInt).toOption.fold[Cell<int>](NM) { Value(_) },
    NM
  )

  Column<int> cast(TypedColumn col) {
    var column = col.column
    var runtimeClass = col.classTagA.runtimeClass
    runtimeClass match {
      case Classes.Byte => column.asInstanceOf[Column<byte>] map (_.toInt)
      case Classes.Short => column.asInstanceOf[Column<Short>] map (_.toInt)
      case Classes.Int => column.asInstanceOf[Column<int>]
      case _ => column flatMap castValue
    }
  }
}

private[framian] final class LongColumnTyper extends NumericColumnTyper<long> {
  private def safeToLong<A>(n: A)(implicit f: A => ScalaNumericAnyConversions): Option<long> = {
    var m = f(n).toLong
    if (n == m) Some(m) else None
  }

  def castValue(x: Any): Option<long> =
    foldValue(x)(
      Some(_),
      safeToLong(_),
      safeToLong(_),
      safeToLong(_),
      { n =>
        if (n.isWhole) {
          var n0 = n.numerator
          var m = n0.toLong
          if (m == n0) Some(m) else None
        } else None
      },
      n => Try(java.lang.Long.parseLong(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column<long>] =
    foldColumn(col)(
      col => Some(col),
      col => None,
      col => None,
      col => None,
      col => None,
      col => None, // TODO: why are these all None?
      None
    )
}

private[framian] final class FloatColumnTyper extends ColumnTyper[Float] {
  private var doubleTyper = new DoubleColumnTyper
  def cast(col: TypedColumn[_]): Column[Float] =
    doubleTyper.cast(col) map (_.toFloat)
}

private[framian] final class DoubleColumnTyper extends NumericColumnTyper[Double] {
  def castValue(x: Any): Option[Double] =
    foldValue(x)(
      n => Some(n.toDouble),
      n => Some(n),
      n => Some(n.toDouble),
      n => Some(n.toDouble),
      n => Some(n.toDouble),
      n => Try(java.lang.Double.parseDouble(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Double]] =
    foldColumn(col)(
      col => Some(col map (_.toDouble)),
      col => Some(col),
      col => Some(col map (_.toDouble)),
      col => Some(col map (_.toDouble)),
      col => Some(col map (_.toDouble)),
      col => Some(col map (java.lang.Double.parseDouble(_))),
      None
    )
}

private[framian] final class BigIntTyper extends NumericColumnTyper[BigInt] {
  def castValue(x: Any): Option[BigInt] =
    foldValue(x)(
      n => Some(BigInt(n)),
      n => if (n.isWhole) Some(BigDecimal(n).toBigInt) else None,
      n => Some(n),
      n => if (n.isWhole) Some(n.toBigInt) else None,
      n => if (n.isWhole) Some(n.numerator) else None,
      n => Try(BigInt(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[BigInt]] =
    foldColumn(col)(
      col => Some(col map (BigInt(_))),
      col => None,
      col => Some(col),
      col => None,
      col => None,
      col => None,
      None
    )
}

private[framian] final class BigDecimalTyper extends NumericColumnTyper[BigDecimal] {
  def castValue(x: Any): Option[BigDecimal] =
    foldValue(x)(
      n => Some(BigDecimal(n)),
      n => Some(BigDecimal(n)),
      n => Some(BigDecimal(n)),
      n => Some(n),
      n => Try(n.toBigDecimal).toOption,
      n => Try(BigDecimal(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[BigDecimal]] =
    foldColumn(col)(
      col => Some(col map (BigDecimal(_))),
      col => Some(col map (BigDecimal(_))),
      col => Some(col map (BigDecimal(_))),
      col => Some(col),
      col => None,
      col => None,
      None
    )
}

private[framian] final class RationalTyper extends NumericColumnTyper[Rational] {
  def castValue(x: Any): Option[Rational] =
    foldValue(x)(
      n => Some(Rational(n)),
      n => Some(Rational(n)),
      n => Some(Rational(n)),
      n => Some(Rational(n)),
      n => Some(n),
      n => Try(Rational(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Rational]] =
    foldColumn(col)(
      col => Some(col map (Rational(_))),
      col => Some(col map (Rational(_))),
      col => Some(col map (Rational(_))),
      col => Some(col map (Rational(_))),
      col => Some(col),
      col => Some(col map (Rational(_))),
      None
    )
}

private[framian] final class NumberTyper extends NumericColumnTyper[Number] {
  def castValue(x: Any): Option[Number] =
    foldValue(x)(
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Some(Number(n)),
      n => Try(Number(n)).toOption,
      None
    )

  def castColumn(col: TypedColumn[_]): Option[Column[Number]] =
    foldColumn(col)(
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      col => Some(col map (Number(_))),
      None
    )
}
*/
