part of frame.csv;

class CsvError {
  final String message;
  final int rowStart;
  final int pos;
  final String context;
  final int row;
  final int col;

  CsvError(
      this.message, this.rowStart, this.pos, this.context, this.row, this.col);

  String description() {
    var msg = "Error parsing CSV row: $message";
    var prefix = "Row $row: ";
    var padLength = col - 1 + prefix.length;
    var pointer = (" " * padLength) + "^";

    return "$msg\n\n$prefix$context\n$pointer";
  }
}
