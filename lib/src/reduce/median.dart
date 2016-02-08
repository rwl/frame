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

part of frame.reduce;

class Median<A extends Field, Order, ClassTag> extends SimpleReducer<A, A> {
//  implicit def chooseRandomPivot(arr: Array[A]): A = arr(scala.util.Random.nextInt(arr.size))

  Tuple2<A, A> findKMedian(
      List<A> arr,
      Either<int, A> kOrValue,
      Either<int, A> k2OrValue,
      /*implicit*/ A choosePivot(Array<A> a)) {
    val a = choosePivot(arr);
    if (kOrValue is Right && k2OrValue is Right) {
      var v1 = kOrValue, v2 = k2OrValue;
      return new Tuple2(v1, v2);
    } else if (kOrValue is Left && k2OrValue is Left) {
      var k = kOrValue, k2 = k2OrValue;
      var s, b = arr.partition((x) => a > x);
      if (s.size == k) {
        findKMedian(arr, new Right(a), new Left(k2));
      } else if (s.size == k2) {
        findKMedian(arr, Left(k), Right(a));
      } else if (s.isEmpty) {
        var s, b = arr.partition((b) => a == b);
        if (s.size > k && s.size > k2) {
          return new Tuple2(a, a);
        } else if (s.size > k) {
          return findKMedian(arr, new Right(a), new Left(k2));
        } else if (s.size > k2) {
          return findKMedian(arr, new Left(k), new Right(a));
        } else {
          return findKMedian(b, new Left(k - s.size), new Left(k2 - s.size));
        }
      } else if (s.size < k && s.size < k2) {
        return findKMedian(b, new Left(k - s.size), new Left(k2 - s.size));
      } else if (s.size < k) {
        return findKMedian(b, new Left(k - s.size), new Left(k2));
      } else if (s.size < k2) {
        return findKMedian(b, new Left(k), new Left(k2 - s.size));
      } else {
        return findKMedian(s, new Left(k), new Left(k2));
      }
    } else if (kOrValue is Left && k2OrValue is Right) {
      var k = kOrValue, v = k2OrValue;
      var s, b = arr.partition((x) => a > x);
      if (s.size == k) {
        return new Tuple2(a, v);
      } else if (s.isEmpty) {
        var s, b = arr.partition((b) => a == b);
        if (s.size > k) {
          return new Tuple2(a, v);
        } else {
          return findKMedian(b, Left(k - s.size), k2OrValue);
        }
      } else if (s.size < k) {
        return findKMedian(b, Left(k - s.size), k2OrValue);
      } else {
        return findKMedian(s, Left(k), k2OrValue);
      }
    } else if (kOrValue is Right && k2OrValue is Left) {
      var v = kOrValue, k = k2OrValue;

      var s, b = arr.partition((x) => a > x);
      if (s.size == k) {
        return new Tuple2(v, a);
      } else if (s.isEmpty) {
        var s, b = arr.partition((b) => a == b);
        if (s.size > k) {
          return new Tuple2(v, a);
        } else {
          return findKMedian(b, kOrValue, new Left(k - s.size));
        }
      } else if (s.size < k) {
        return findKMedian(b, kOrValue, new Left(k - s.size));
      } else {
        findKMedian(s, kOrValue, new Left(k));
      }
    }
  }

  Median(List<A> arr, /*implicit*/ A choosePivot(List<A> l)) {
    if (arr.size % 2 == 0) {
      var left,
          right =
          findKMedian(arr, Left(arr.size / 2), Left((arr.size / 2) - 1));
      return (left / 2) + (right / 2);
    } else {
      findKMedian(arr, Left((arr.size - 1) / 2), Left((arr.size - 1) / 2)).v1;
    }
  }

  // TODO: Use Spire's qselect/qmin instead?
  // def findMedianSpire(data: Array[A]): A = {
  //   val lower = data.qselect(data.size / 2)
  //   if (data.size % 2 == 0) {
  //     val upper = data.qselect(data.size / 2 + 1)
  //     (lower + upper) / 2
  //   } else {
  //     lower
  //   }
  // }

  Cell<A> reduce(List<A> data) =>
      data.isEmpty ? NA : new Value(findMedian(data));
}
