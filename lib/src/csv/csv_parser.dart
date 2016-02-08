part of frame.csv;

class CsvParser {
  final CsvFormat format;

  CsvParser(this.format);

  String _removeRowDelim(String context) {
    Option<String> dropTail(String tail) {
      if (context.endsWith(tail)) {
        return new Some(context.dropRight(tail.length));
      } else {
        return new None();
      }
    }

    return dropTail(format.rowDelim.value)
        .orElse(format.rowDelim.alternate.flatMap(dropTail))
        .getOrElse(context);
  }

  Csv parseResource(a, close(a), Option<String> read(a)) {
    Csv loop(ParserState s0, Option<Fail> fail, int row, List acc) {
      var s1, instr = parse(s0);

      if (instr is Emit) {
        var cells = instr.cells;
        loop(s1, fail, row + 1, acc /*:*/ + Right(cells));
      } else if (instr is Fail) {
        var f = instr;
        loop(s1, new Some(f), row, acc);
      } else if (instr is Resume) {
        if (fail is Some && fail.get is Fail) {
          var msg = fail.get.msg, pos = fail.get.pos;
          var context =
              removeRowDelim(s1.input.substring(s0.rowStart, s1.rowStart));
          var error = CsvError(
              msg, s0.rowStart, pos, context, row, pos - s0.rowStart + 1);
          loop(s1, None, row + 1, acc /*:*/ + Left(error));
        } else if (fail is None) {
          loop(s1, None, row, acc);
        }
      } else if (instr is NeedInput) {
        var ret = read(a);
        if (ret is Some) {
          var chunk = ret.get;
          loop(s1.mapInput((i) => i.append(chunk)), fail, row, acc);
        } else if (ret is None) {
          loop(s1.mapInput((i) => i.finished), fail, row, acc);
        }
      } else if (instr is Done) {
        var csv = UnlabeledCsv(format, acc);
        return format.header ? csv.labeled : csv;
      }
    }

    try {
      read(a).map((input0) {
        loop(ParseRow(0, 0, Input.init(input0)), None, 1, []);
      }).getOrElse(() {
        return new Csv.empty(format);
      });
    } finally {
      try {
        close(a);
      } on Exception catch (_) {
        // Do nothing - hopefully letting original exception through.
      }
    }
  }

  Csv parseReader(Reader reader) {
    var buffer = new List(Csv.BufferSize);
    parseResource /*[Reader]*/ (reader, (r) => r.close(), (reader) {
      var len = reader.read(buffer);
      if (len >= 0) {
        return new Some(new String(buffer, 0, len));
      } else {
        return new None();
      }
    });
  }

  Csv parseInputStream(InputStream stream, [Charset charset]) {
    if (charset == null) {
      charset = StandardCharsets.UTF_8;
    }
    return parseReader(new InputStreamReader(stream, charset));
  }

  Csv parseFile(File file, [Charset charset]) {
    if (charset == null) {
      charset = StandardCharsets.UTF_8;
    }
    return parseInputStream(new FileInputStream(file), charset);
  }

  Csv parseString(String input) {
    Option<String> next = new Some(input);
    parseResource /*[Unit]*/ (() => null, (a) => null, (_) {
      var chunk = next;
      next = None;
      chunk;
    });
  }

  Tuple2<ParserState, Instr<CsvRow>> _parse(ParserState state) {
    Input input = state.input;
    int pos = state.readFrom;
    Char ch() => input.charAt(pos);
    bool endOfInput() => pos >= input.length;
    bool endOfFile() => endOfInput && input.isLast;
    advance([int i = 1]) => pos += i;
    retreat([int i = 1]) => pos -= i;

    Function isFlag(String str) {
      int loop(int i) {
        if (i >= str.length) {
          retreat(i);
          return i;
        } else if (endOfInput) {
          retreat(i);
          return endOfFile ? 0 : -1;
        } else if (str.charAt(i) == ch) {
          advance();
          return loop(i + 1);
        } else {
          retreat(i);
          return 0;
        }
      }

      return () => loop(0);
    }

    Function either(int f0(), int f1()) {
      return () {
        var i = f0();
        return i == 0 ? f1() : i;
      };
    }

    var isQuote = isFlag(quote);
    var isQuoteEscape = isFlag(quoteEscape);
    var isSeparator = isFlag(separator);
    var isRowDelim = rowDelim.alternate.map((alt) {
      either(isFlag(rowDelim.value), isFlag(alt));
    }).getOrElse(isFlag(rowDelim.value));
    var isEndOfCell = either(isSeparator, isRowDelim);
    isEscapedQuote() {
      val e = isQuoteEscape();
      if (e > 0) {
        advance(e);
        val q = isQuote();
        retreat(e);
        if (q > 0) {
          return q + e;
        } else {
          return q;
        }
      } else {
        return e;
      }
    }

    ParseResult<CsvCell> unquotedCell() {
      var start = pos;
      ParseResult<CsvCell> loop() {
        var flag = isEndOfCell();
        if (flag > 0 || endOfFile) {
          var value = input.substring(start, pos);
          var csvCell;
          if (value == empty) {
            csvCell = EmptyCsvCell;
          } else if (value == invalid) {
            csvCell = InvalidCsvCell;
          } else {
            csvCell = new CsvDataCell(value);
          }
          return new Emit(csvCell);
        } else if (flag == 0) {
          advance();
          return loop();
        } else {
          return new NeedInput();
        }
      }

      return loop();
    }

    ParseResult<CsvCell> quotedCell() {
      var start = pos;
      ParseResult<CsvCell> loop() {
        if (endOfInput) {
          if (endOfFile) {
            return new Fail("Unmatched quoted string at end of file", pos);
          } else {
            return new NeedInput();
          }
        } else {
          var d = allowRowDelimInQuotes ? 0 : isRowDelim();
          var e = isEscapedQuote();
          var q = isQuote();

          if (d < 0 || e < 0 || q < 0) {
            return new NeedInput();
          } else if (d > 0) {
            return new Fail("Unmatched quoted string at row delimiter", pos);
          } else if (e > 0) {
            advance(e);
            return loop();
          } else if (q > 0) {
            var escaped =
                input.substring(start, pos).replace(escapedQuote, quote);
            advance(q);
            return new Emit(new CsvDataCell(escaped));
          } else {
            advance(1);
            return loop();
          }
        }
      }

      return loop();
    }

    ParseResult<CsvCell> cell() {
      val q = isQuote();
      if (q == 0) {
        return unquotedCell();
      } else if (q > 0) {
        advance(q);
        return quotedCell();
      } else {
        return new NeedInput();
      }
    }

    bool skipToNextRow() {
      var d = isRowDelim();
      if (d > 0 || endOfFile) {
        advance(d);
        return true;
      } else if (d == 0) {
        advance(1);
        return skipToNextRow();
      } else {
        if (input.isLast) {
          advance(input.length - pos);
        }
        return input.isLast;
      }
    }

    Tuple2<ParserState, Instr<CsvRow>> row(int rowStart, List<CsvCell> cells) {
      var start = pos;
      needInput() =>
          new Tuple2(ContinueRow(rowStart, start, cells, input), NeedInput);

      val s = isSeparator();
      if (s == 0) {
        val r = isRowDelim();
        if (r > 0 || endOfFile) {
          advance(r);
          return new Tuple2(
              ParseRow(pos, pos, input.marked(pos)), Emit(new CsvRow(cells)));
        } else if (r == 0) {
          return new Tuple2(SkipRow(rowStart, pos, input),
              Fail("Expected separator, row delimiter, or end of file", pos));
        } else {
          return needInput();
        }
      } else if (s > 0) {
        advance(s);
        var ret = cell();
        if (ret == Emit) {
          var c = ret.c;
          row(rowStart, cells /*:*/ + c);
        } else if (ret == Fail) {
          var f = ret;
          return new Tuple2(SkipRow(rowStart, pos, input), f);
        } else if (ret == NeedInput) {
          return needInput();
        }
      } else {
        return needInput();
      }
    }

    if (state is ContinueRow) {
      var rowStart = state.rowState,
          readFrom = state.readFrom,
          partial = state.partial;
      return row(rowStart, partial);
    } else if (state is ParseRow) {
      var instr = state;
      var rowStart = state.rowState, readFrom = state.readFrom;
      if (endOfFile) {
        return new Tuple2(instr, Done);
      } else {
        var ret = cell();
        if (ret is Emit) {
          var csvCell = ret.csvCell;
          row(rowStart, Vector(csvCell));
        } else if (ret is Fail) {
          var f = ret;
          return new Tuple2(SkipRow(rowStart, pos, input), f);
        } else if (ret is NeedInput) {
          return new Tuple2(instr, NeedInput);
        }
      }
    } else if (state is SkipRow) {
      var rowStart = state.rowState, readFrom = state.readFrom;
      if (skipToNextRow()) {
        return new Tuple2(ParseRow(pos, pos, input.marked(pos)), Resume);
      } else {
        return new Tuple2(SkipRow(rowStart, pos, input), NeedInput);
      }
    }
  }
}
