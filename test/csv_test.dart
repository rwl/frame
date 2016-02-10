library frame.test.csv;

import 'dart:io';
import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

csvTest() {
  var csvRoot = "csvs/";

  var airPassengers = csvRoot + "AirPassengers-test.csv";
  var airPassengersBadComma = csvRoot + "AirPassengers-badcomma.csv";
  var autoMPG = csvRoot + "auto-mpg-test.tsv";

  var defaultRowIndex = Index.fromKeys([0, 1, 2, 3, 4]);
  var withColumnRowIndex = Index.fromKeys([0, 1, 2, 3]);
  var defaultAPColumnIndex = Index.fromKeys([0, 1, 2]);

  var defaultAirPassengers = new ColOrientedFrame(
      Index.fromKeys([0, 1, 2, 3, 4]),
      Series([
        new Tuple2(
            0,
            new TypedColumn(new Column<int>(
                    [NA, Value(1), Value(2), Value(3), Value(4)]))
                .orElse(new TypedColumn(new Column<String>([Value("")])))),
        new Tuple2(
            1,
            new TypedColumn(new Column<BigDecimal>([
              NA,
              Value(BigDecimal("1949")),
              Value(BigDecimal("1949.08333333333")),
              Value(BigDecimal("1949.16666666667")),
              Value(BigDecimal("1949.25"))
            ])).orElse(
                new TypedColumn(new Column<String>([new Value("time")])))),
        new Tuple2(
            2,
            new TypedColumn(new Column<BigDecimal>([
              NA,
              Value(BigDecimal("112")),
              Value(BigDecimal("118")),
              Value(BigDecimal("132")),
              Value(BigDecimal("129"))
            ])).orElse(
                new TypedColumn(new Column<String>([Value("AirPassengers")]))))
      ]));
  var columnAirPassengers = new Frame.fromRows([
    [1, BigDecimal(1949), 112, HNil],
    [2, BigDecimal(1949.08333333333), 118, HNil],
    [3, BigDecimal(1949.16666666667), 132, HNil],
    [4, BigDecimal(1949.25), 129, HNil]
  ])
      .withColIndex(Index.fromKeys("", "time", "AirPassengers"))
      .withRowIndex(withColumnRowIndex);

  val defaultMPG = Frame.fromRows([
    [
      18.0,
      8,
      307.0,
      130.0,
      3504,
      12.0,
      70,
      1,
      "chevrolet chevelle malibu",
      HNil
    ],
    [15.0, 8, 350.0, 165.0, 3693, 11.5, 70, 1, "buick skylark 320", HNil],
    [18.0, 8, 318.0, 150.0, 3436, 11.0, 70, 1, "plymouth satellite", HNil],
    [16.0, 8, 304.0, 150.0, 3433, 12.0, 70, 1, "amc rebel sst", HNil],
    [17.0, 8, 302.0, 140.0, 3449, 10.5, 70, 1, "ford torino", HNil]
  ])
      .withRowIndex(defaultRowIndex)
      .withColIndex(Index.fromKeys(0, 1, 2, 3, 4, 5, 6, 7, 8));

  group("CsvParser should", () {
    test("parse air passengers as unlabeled CSV", () {
      expect(Csv.parsePath(airPassengers).unlabeled.toFrame(),
          equals(defaultAirPassengers));
    });

    test("parse air passengers as labeled CSV", () {
      expect(Csv.parsePath(airPassengers).labeled.toFrame(),
          equals(columnAirPassengers));
    });

    test("parse autoMPG as unlabeled TSV", () {
      expect(Csv.parsePath(autoMPG).unlabeled.toFrame(), equals(defaultMPG));
    });

    test("parse CSV with separator in quote", () {
      var data = 'a,"b","c,d"|"e,f,g"';
      var csv = Csv.parseString(data, CsvFormat.Guess.withRowDelim("|"));
      var frame = csv.unlabeled.toFrame();
      expect(
          frame.getRow(0),
          equals(Some(Rec([
            new Tuple2(0, "a"),
            new Tuple2(1, "b"),
            new Tuple2(2, "c,d")
          ]))));
      expect(frame[String](1, 0), equals(Value("e,f,g")));
      expect(frame[String](1, 1), equals(NA));
      expect(frame[String](1, 2), equals(NA));
    });

    var TestFormat = new CsvFormat(
        separator: ",",
        quote: "'",
        quoteEscape: "'",
        empty: "N/A",
        invalid: "N/M",
        header: false,
        rowDelim: CsvRowDelim.Custom("|"),
        allowRowDelimInQuotes: true);

    test("parse escaped quotes", () {
      expect(
          Csv.parseString("a,'''','c'''|'''''d''''', ''''", TestFormat).rows(),
          equals(Vector(Right(CsvRow(Vector(Data("a"), Data("'"), Data("c'")))),
              Right(CsvRow(Vector(Data("''d''"), Data(" ''''")))))));
    });

    test("respect CsvFormat separator", () {
      expect(
          Csv.parseString("a,b,c|d,e,f", TestFormat).rows,
          equals(Csv
              .parseString("a;b;c|d;e;f", TestFormat.withSeparator(";"))
              .rows));
    });

    test("respect CsvFormat quote", () {
      expect(
          Csv.parseString("'a,b','b'|d,e", TestFormat).rows,
          equals(Csv
              .parseString("^a,b^,^b^|d,e", TestFormat.withQuote("^"))
              .rows));
    });

    test("respect CsvFormat quote escape", () {
      expect(
          Csv.parseString("'a''b',''''|' '", TestFormat).rows,
          equals(Csv
              .parseString(
                  "'a\\'b','\\''|' '", TestFormat.withQuoteEscape("\\"))
              .rows));
    });

    test("respect CsvFormat empty", () {
      expect(Csv.parseString("a,N/A,b|N/A,N/A", TestFormat).rows,
          equals(Csv.parseString("a,,b|,", TestFormat.withEmpty("")).rows));
    });

    test("respect CsvFormat invalid", () {
      expect(
          Csv.parseString("a,N/M,b|N/M,N/M", TestFormat).rows,
          equals(Csv
              .parseString("a,nm,b|nm,nm", TestFormat.withInvalid("nm"))
              .rows));
    });

    test("respect CsvFormat row delimiter", () {
      expect(
          Csv.parseString("a,b|c,d|e,f", TestFormat).rows,
          equals(Csv
              .parseString(
                  "a,b\nc,d\ne,f", TestFormat.withRowDelim(CsvRowDelim.Unix))
              .rows));
    });

    test("parse CSV with row delimiter in quote", () {
      expect(
          Csv.parseString("a,'b|c'|'d|e',f", TestFormat).rows,
          equals(Vector(Right(CsvRow(Vector(Data("a"), Data("b|c")))),
              Right(CsvRow(Vector(Data("d|e"), Data("f")))))));
    });

    test("parser respects whitespace", () {
      var data = " a , , 'a','b'|  b  ,c  ,   ";
      var csv = Csv.parseString(data, CsvFormat.Guess.withRowDelim("|"));
      expect(
          csv.rows,
          equals(Vector(
              Right(CsvRow(
                  Vector(Data(" a "), Data(" "), Data(" 'a'"), Data("b")))),
              Right(CsvRow(Vector(Data("  b  "), Data("c  "), Data("   ")))))));
    });
  });
}
