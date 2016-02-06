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

class EvalColumn<A> extends BoxedColumn<A> {
  Function f;

  EvalColumn(Cell<A> f(int a));

  @override
  Cell<A> apply(int row) => f(row);

  Column<B> cellMap(Cell<B> g(Cell<A> c)) => EvalColumn(f.andThen(g));

  Column<A> reindex(List<int> index) =>
      DenseColumn.force(EvalColumn(index.andThen(f)), index.length);

  Column<A> force(int len) => DenseColumn.force(this, len);

  Column<A> mask(Mask mask) => EvalColumn((row) {
        return mask(row) ? NA : f(row);
      });

  Column<A> setNA(int naRow) => EvalColumn((row) {
        return (row == naRow) ? NA : f(row);
      });

  Column<A> memoize(bool optimistic) {
    if (optimistic) {
      return new OptimisticMemoizingColumn(f);
    } else {
      return new PessimisticMemoizingColumn(f);
    }
  }

  Column orElse(Column that) {
    return EvalColumn((row) {
//      f(row) match {
//        case NM => that(row) match {
//          case NA => NM
//          case cell => cell
//        }
//        case NA => that(row)
//        case cell => cell
//      }
    });
  }

  Column<A> shift(int n) {
    return EvalColumn((row) {
      try {
        return f(Checked.minus(row, n));
      } on ArithmeticOverflowException catch (_) {
        // If we overflow, then it means that `row - n` overflowed and, hence,
        // wrapped around. Since `shift` is meant to just shift rows, and not
        // wrap them back around, we return an NA. So, if we have a `Column`
        // defined for all rows, and shift it forward 1 row, then
        // `Column(Int.MinValue)` should return `NA`.
        return NA;
      }
    });
  }

  Column zipMap(Column that, dynamic f(A a, B b)) {
//    that match {
//    case (that: DenseColumn[_]) =>
//      DenseColumn.zipMap[A, B, C](this.force(that.values.length).asInstanceOf[DenseColumn<A>], that.asInstanceOf[DenseColumn<B>], f)
//    case _ =>
//      EvalColumn { row =>
//        (this(row), that(row)) match {
//          case (Value(a), Value(b)) => Value(f(a, b))
//          case (NA, _) | (_, NA) => NA
//          case _ => NM
//        }
//      }
  }
}
