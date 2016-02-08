part of frame.csv;

abstract class ParserState {
  int get rowStart;
  int get readFrom;
  Input get input;

  ParserState withInput(Input input0) {
    if (this is ContinueRow) {
      var partial = this.partial;
      return new ContinueRow(rowStart, readFrom, partial, input0);
    } else if (this is SkipRow) {
      return new SkipRow(rowStart, readFrom, input0);
    } else {
      return new ParseRow(rowStart, readFrom, input0);
    }
  }

  ParserState mapInput(Input f(Input input)) => withInput(f(input));
}

class ContinueRow extends ParserState {
  final int rowStart;
  final int readFrom;
  final List<CsvCell> partial;
  final Input input;
  ContinueRow(this.rowStart, this.readFrom, this.partial, this.input);
}

class SkipRow extends ParserState {
  final int rowStart;
  final int readFrom;
  final Input input;
  SkipRow(this.rowStart, this.readFrom, this.input);
}

class ParseRow extends ParserState {
  final int rowStart;
  final int readFrom;
  final Input input;
  ParseRow(this.rowStart, this.readFrom, this.input);
}
