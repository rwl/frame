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

class Outliers<A extends Field, Order, ClassTag>
    extends SimpleReducer<A, Tuple2<Option<A>, Option<A>>> {
  final double k;
  Outliers([this.k = 1.5]);

  var quantiler = new Quantile<A>([0.25, 0.75]);

  Value<Tuple2<Option<A>, Option<A>>> reduce(List<A> data) {
//    var (_, q1) :: (_, q3) :: Nil = quantiler.quantiles(data)

    var iqr = q3 - q1;
    var lowerFence = q1 - (k * iqr);
    var upperFence = q3 + (k * iqr);

    var lowerOutliers = data.filter((d) => d <= lowerFence);
    var upperOutliers = data.filter((d) => d >= upperFence);

    return Value(new Tuple2(
        (lowerOutliers.length > 0) ? new Some(lowerFence) : new None(),
        (upperOutliers.length > 0) ? new Some(upperFence) : new None()));
  }
}
