part of frame.csv;

abstract class CsvCell {
  String render(CsvFormat format);

  factory CsvCell.fromNonValue(NonValue nonValue) {
    if (nonValue == NA) {
      return EmptyCsvCell;
    } else {
      return InvalidCsvCell;
    }
  }

  static /*implicit*/ ColumnTyper<CsvCell> CsvCellColumnTyper =
      new _CsvCellColumnTyper();
}

class CsvDataCell implements CsvCell {
  final String value;
  CsvDataCell(this.value);

  String render(CsvFormat format) => format.render(value);

  @override
  String toString() => value;
}

const _EmptyCsvCell EmptyCsvCell = const _EmptyCsvCell();

class _EmptyCsvCell implements CsvCell {
  const _EmptyCsvCell();

  String render(CsvFormat format) => format.empty;

  @override
  String toString() => "-";
}

const _InvalidCsvCell InvalidCsvCell = const _InvalidCsvCell();

class _InvalidCsvCell implements CsvCell {
  const _InvalidCsvCell();

  String render(CsvFormat format) => format.invalid;

  @override
  String toString() => "<error>";
}

class _CsvCellColumnTyper {
  //implements ColumnTyper<CsvCell> {
  Column<CsvCell> cast(TypedColumn col) {
//    var num = col.cast[BigDecimal].map((n) => Data(n.toString): CsvCell);
//    var text = col.cast[String].map((a) => Data(a): CsvCell);
//    var any = col.cast[Any].map((any) => Data(any.toString): CsvCell);
//    num |+| text |+| any
  }
}
