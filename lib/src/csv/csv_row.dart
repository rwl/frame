part of frame.csv;

/// A single row in a CSV file.
class CsvRow {
  //extends AnyVal {
  final List<CsvCell> cells;
  CsvRow(this.cells);

  List<String> text(CsvFormat format) => cells.map((c) => c.render().format());

  String render(CsvFormat format) => cells
      .iterator()
      .map((c) => c.render().format())
      .mkString(format.separator);

  @override
  String toString() => cells.mkString("CsvRow(", ", ", ")");

//  CsvRow call(List<CsvCell> cells) => new CsvRow(cells);

  static /*implicit*/ RowExtractor<CsvRow, Col,
      Variable> csvRowExtractor /*[Col]*/ () {
    RowExtractor.collectionOf /*[Vector, CsvCell, Col]*/ .map((cells) {
      return CsvRow(cells.map((c) =>
          c.fold /*[CsvCell]*/ (EmptyCsvCell, InvalidCsvCell, (cell) => cell)));
    });
  }
}
