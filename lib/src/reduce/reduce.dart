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

library frame.reduce;

import '../../frame.dart';

part 'reducer.dart';
part 'count.dart';
part 'first_and_last.dart';
part 'max.dart';
part 'mean.dart';
part 'median.dart';
part 'monoid_reducer.dart';
part 'outliers.dart';
part 'predicates.dart';
part 'quantile.dart';
part 'semigroup_reducer.dart';
part 'simple_reducer.dart';

final Reducer<A, A> Sum /*<A extends AdditiveMonoid>*/ =
    new MonoidReducer(new Monoid.additive<A>());
