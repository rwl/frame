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

class First<A> implements Reducer<A, A> {
  Cell<A> reduce(Column<A> column, List<int> indices, int start, int end) {
    Cell<A> loop(int i) {
      if (i < end) {
        var row = indices[i];
        var first = column.apply(row);
        return first == NA ? loop(i + 1) : first;
      } else {
        return NA;
      }
    }

    return loop(start);
  }
}

class Last<A> implements Reducer<A, A> {
  Cell<A> reduce(Column<A> column, List<int> indices, int start, int end) {
    Cell<A> loop(int i) {
      if (i >= start) {
        var row = indices[i];
        var last = column.apply(row);
        return last == NA ? loop(i - 1) : last;
      } else {
        return NA;
      }
    }

    return loop(end - 1);
  }
}

class FirstN<A> implements Reducer<A, List<A>> {
  final int n;
  FirstN(this.n) {
    if (n <= 0) {
      throw new ArgumentError(
          "new FirstN(n = $n), but n must be greater than 0");
    }
  }

  Cell<List<A>> reduce(
      Column<A> column, List<int> indices, int start, int end) {
    var rows = <A>[];
    var k = 1;

    var success = column.forEach(start, end, (i) => indices(i), (_, value) {
      rows.add(value);
      if (k == n) {
        return new Value(rows.result());
      }
      k += 1;
    });

    return success ? NA : NM;
  }
}

class LastN<A> extends Reducer<A, List<A>> {
  final int n;
  LastN(this.n) {
    if (n <= 0) {
      throw new ArgumentError(
          "new LastN(n = $n), but n must be greater than 0");
    }
  }

  Cell<List<A>> reduce(
      Column<A> column, List<int> indices, int start, int end) {
    var rows = <A>[];
    var k = 1;

    // TOODO: The end - i + start - 1 is rather unsatisifying.
    var success = column.forEach(
        start, end, (i) => indices(end - i + start - 1), (_, value) {
      rows.insert(0, value);
      if (k == n) {
        return new Value(rows);
      }
      k += 1;
    });

    return success ? NA : NM;
  }
}
