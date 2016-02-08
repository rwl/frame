part of frame.csv;

class Input {
  final int offset;
  final String data;
  final bool isLast;
  final int mark;

  Input(this.offset, this.data, this.isLast, this.mark);

  int _check(int i) {
    if ((i < offset) || (i > (offset + data.length))) {
      throw new IndexOutOfBoundsException();
    } else {
      var j = i - offset;
      if (j <= Int.MaxValue) {
        return j.toInt();
      } else {
        throw new IndexOutOfBoundsException();
      }
    }
  }

  Char charAt(int i) => data.charAt(_check(i));

  int get length => offset + data.length;

  String substring(int from, int until) =>
      data.substring(check(from), check(until));

  Input marked(int pos) => new Input(offset, data, isLast, pos);

  Input _trim() {
    if (mark > offset) {
      var next = math.min(mark - offset, data.length.toLong).toInt();
      var tail = data.substring(next);
      var offset0 = offset + next;
      return new Input(offset0, tail, isLast, offset0);
    } else {
      return this;
    }
  }

  Input append(String chunk, [bool last = false]) {
    if (mark > offset) {
      return trim.append(chunk, last);
    } else if (chunk.isEmpty) {
      return new Input(offset, data, last, mark);
    } else {
      return new Input(offset, data + chunk, last, mark);
    }
  }

  Input get finished => new Input(offset, data, true, mark);

  factory Input.fromString(String str) => new Input(0, str, true, 0);

  factory Input.init(String str) => new Input(0, str, false, 0);
}
