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

abstract class _Count implements Reducer<dynamic, int> {
  Cell<int> reduce(Column column, List<int> indices, int start, int end) {
    var n = 0;
    var success =
        column.forEach(start, end, (i) => indices[i], (_, __) => n += 1);
    return success ? new Value(n) : NM;
  }
}
