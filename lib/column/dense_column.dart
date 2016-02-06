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

abstract class DenseColumn<A> extends UnboxedColumn<A> {
  DenseColumn();

  List get values;
  Mask get naValues;
  Mask get nmValues;

  _valid(int row) => row >= 0 && row < values.length;
  bool isValueAt(int row) => _valid(row) && !naValues[row] && !nmValues[row];
  NonValue nonValueAt(int row) => nmValues[row] ? NM : NA;
}

class GenericColumn<A> extends DenseColumn<A> {
  List<A> values;
  Mask naValues;
  Mask nmValues;

  GenericColumn(this.values, this.naValues, this.nmValues) : super();

  A valueAt(int row) => values[row];
}
