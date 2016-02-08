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

class Quantile<A extends Field, Order, ClassTag>
    extends SimpleReducer<A, List<Tuple2<Double, A>>> {
  final Iterable<double> percentiles;
  Quantile([this.percentiles = const [0.0, 0.25, 0.5, 0.75, 1.0]]) {
    require(percentiles.forall((p) => p >= 0.0 && p <= 1.0),
        "percentile must lie in [0,1]");
  }

  Cell<List<Tuple2<double, A>>> reduce(List<A> data) {
    if (data.length == 0 && percentiles.length > 0) {
      return NA;
    } else {
      return Value(quantiles(data));
    }
  }

  List<Tuple2<double, A>> quantiles(List<A> s) {
    var as = s.sorted;

    percentiles.map((p) {
      var i = p * (as.length - 1);
      var lb = i.toInt();
      var ub = math.ceil(i).toInt();
      var w = Field[A].fromDouble(i - lb);
      var value = as(lb) * (1 - w) + as(ub) * w;
      return new Tuple2(p, value);
    }).toList();
  }
}
