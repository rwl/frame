library frame.csv;

import 'package:option/option.dart';

import '../../frame.dart';

part 'csv_cell.dart';
part 'csv_error.dart';
part 'csv_format.dart';
part 'csv_parser.dart';
part 'csv_row.dart';
part 'input.dart';
part 'parse_result.dart';
part 'parser_state.dart';

abstract class Csv {
  final CsvFormat format;
  final List /*[Either[CsvError, CsvRow]]*/ rows;

  Csv(this.format, this.rows);

  /*lazy*/ List<CsvRow> get data => rows.collect((row) {
        if (row is Right) {
          return row;
        }
      });

  /*lazy*/ List<CsvError> get errors => rows.collect((error) {
        if (error is Left) {
          return error;
        }
      });

  bool get hasErrors => !errors.isEmpty;

  UnlabeledCsv get unlabeled {
    if (this is /*@*/ UnlabeledCsv) {
      return this;
    } else if (this is LabeledCsv) {
      return new UnlabeledCsv(this.format.copy(header: false), this.rows);
    }
  }

  LabeledCsv get labeled {
    if (this is /*@*/ LabeledCsv) {
      return this;
    } else if (this is UnlabeledCsv) {
      var format0 = this.format.copy(header: true);
      rows.headOption.flatMap((r) => r.right.toOption()).map((hdr) {
        return new LabeledCsv(format0, hdr.text(format), this.rows.tail);
      }).getOrElse(() {
        return new LabeledCsv(format0, [], []);
      });
    }
  }

  @override
  String toString() {
    var full;
    if (this is LabeledCsv) {
//        full = new CsvRow(this.header.map((h) => (new CsvCell.Data(h))) +: data
    } else if (this is UnlabeledCsv) {
      full = data;
    }

    return full.iterator
        .map((f) => f.render(format))
        .mkString(format.rowDelim.value);
  }

  static final int BufferSize = 32 * 1024;

  factory Csv.empty(CsvFormat format) {
    if (format.header) {
      return new LabeledCsv(format, [], []);
    } else {
      return new UnlabeledCsv(format, []);
    }
  }

  static Frame<int, int> toFrame(List<CsvRow> rows) {
    var cols = rows.foldLeft(
        new Map<int,
            Tuple2<ColumnBuilder<BigDecimal>, ColumnBuilder<String>>>.empty(),
        (acc0, row) {
      row.cells.zipWithIndex.foldLeft(acc0, (acc, tup /*(cell, colIdx)*/) {
        var numCol,
            strCol = acc.getOrElse(
                colIdx,
                new Tuple2(new ColumnBuilder<BigDecimal>(),
                    new ColumnBuilder<String>()));
        if (cell is CsvCell.Data) {
          var value = cell;
          numCol += scala.util
              .Try(BigDecimal(value))
              .map((v) => Value(v))
              .getOrElse(NA);
          strCol.addValue(value);
        } else if (cell is CsvCell.Empty) {
          numCol.addNA();
          strCol.addNA();
        } else if (cell is CsvCell.Invalid) {
          numCol.addNM();
          strCol.addNM();
        }
        acc + new Tuple2(colIdx, new Tuple2(numCol, strCol));
      });
    });

    var columns = new Column.eval(cols.map((col, tup /*(numCol, strCol)*/) {
      return new Tuple2(
          col,
          new Value(new TypedColumn(numCol.result())
              .orElse(new TypedColumn(strCol.result()))));
    }));

    return new ColOrientedFrame(new Index(range(0, rows.size)),
        new Index(range(0, cols.size)), columns);
  }

  factory Csv.fromFrame(CsvFormat format, Frame<dynamic, Col> frame) {
    var rows = frame
        .get(Cols.all[Col].as[CsvRow])
        .denseIterator
        .map((_, row) => new Right(row))
        .toVector();

    if (format.header) {
      var header = frame.colIndex.toVector.map((i) => i.v1.toString());
      return LabeledCsv(format, header, rows);
    } else {
      return UnlabeledCsv(format, rows);
    }
  }

  factory Csv.parseReader(Reader reader, [CsvFormatStrategy format]) {
    if (format == null) {
      format = CsvFormat.Guess;
    }
    var format0, reader0;
    if (format == GuessCsvFormat) {
      var tup = format.guess(reader);
      format0 = tup.v1;
      reader0 = tup.v2;
    } else {
      format0 = format;
      reader0 = reader;
    }
    return new CsvParser(format0).parseReader(reader0);
  }

  factory Csv.parseString(String input, [CsvFormatStrategy format]) {
    if (format == null) {
      format = CsvFormat.Guess;
    }
    return parseReader(new StringReader(input), format);
  }

  factory Csv.parseInputStream(InputStream stream,
      [CsvFormatStrategy format, Charset charset]) {
    if (format == null) {
      format = CsvFormat.Guess;
    }
    if (charset == null) {
      charset = StandardCharsets.UTF_8;
    }
    return parseReader(new InputStreamReader(istream, charset), format);
  }

  factory Csv.parseFile(File file,
      [CsvFormatStrategy format, Charset charset]) {
    if (format == null) {
      format = CsvFormat.Guess;
    }
    if (charset == null) {
      charset = StandardCharsets.UTF_8;
    }
    return parseInputStream(new FileInputStream(file), format, charset);
  }

  factory Csv.parsePath(String filename,
      [CsvFormatStrategy format, Charset charset]) {
    if (format == null) {
      format = CsvFormat.Guess;
    }
    if (charset == null) {
      charset = StandardCharsets.UTF_8;
    }
    return parseFile(new File(filename), format, charset);
  }
}

class LabeledCsv extends Csv {
  final List<String> header;

  LabeledCsv(CsvFormat format, this.header, List rows) : super(format, rows);

  Frame<int, String> toFrame() =>
      Csv.toFrame(data).withColIndex(new Index.fromKeys(header /*: _**/));
}

class UnlabeledCsv extends Csv {
  UnlabeledCsv(CsvFormat format, List rows) : super(format, rows);

  Frame<int, int> toFrame() => Csv.toFrame(data);
}
