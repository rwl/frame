part of frame.csv;

class CsvRowDelim {
  final String value;
  final Option<String> alternate;
  const CsvRowDelim(this.value, [this.alternate = const None()]);

  static const Unix = const CsvRowDelim("\n");
  static const Windows = const CsvRowDelim("\r\n");
  static const Both = const CsvRowDelim("\n", const Some("\r\n"));
}

abstract class CsvFormatStrategy {
  CsvFormatStrategy withSeparator(String separator);
  CsvFormatStrategy withQuote(String quote);
  CsvFormatStrategy withQuoteEscape(String quoteEscape);
  CsvFormatStrategy withEmpty(String empty);
  CsvFormatStrategy withInvalid(String invalid);
  CsvFormatStrategy withHeader(bool header);
  CsvFormatStrategy withCsvRowDelim(CsvRowDelim rowDelim);
  CsvFormatStrategy withRowDelim(String rowDelim);
}

abstract class GuessCsvFormat implements CsvFormatStrategy {
  /// Makes a guess at the format of the CSV accessed by `reader`. This
  /// returns the format, as well as the a new pushback reader to be used
  /// in place of `reader`. The original reader will have some data read
  /// out of it. The returned reader will contain all the original reader's
  /// data.
  Tuple2<CsvFormat, Reader> apply(Reader reader) {
    var reader0 = new PushbackReader(reader, Csv.BufferSize);
    var buffer = new List(Csv.BufferSize);
    var len = reader0.read(buffer);
    reader0.unread(buffer, 0, len);

    val chunk = new String(buffer, 0, len);
    val format = call(chunk);
    return new Tuple2(format, reader0);
  }

  /// Given the first part of a CSV file, return a guess at the format.
  CsvFormat call(String str);
}

class CsvFormat extends CsvFormatStrategy {
  /// The delimiter that separates fields within the rows.
  final String separator;

  /// The character/string that indicates the beginning/end of a quoted value.
  final String quote = "\"";

  /// The string that is used to escape a quote character, within a quote.
  final String quoteEscape = "\"";

  /// The value of an empty field (common values are - or ?).
  final String empty = "";

  /// The value of an invalid field. Empty values take precedence, so setting
  /// this to the same value as `empty` essentially disabled invalid values.
  final String invalid = "";

  /// Indicates whether or not the CSV's first row is actually a header.
  final bool header = false;

  /// The delimiter used to separate row.
  final CsvRowDelim rowDelim = CsvRowDelim.Both;

  /// If true, allow row delimiters within quotes, otherwise they are treated
  /// as an error.
  final bool allowRowDelimInQuotes = true;

  final String escapedQuote;

  CsvFormat(separator,
      [this.quote = "\"",
      this.quoteEscape = "\"",
      this.empty = "",
      this.invalid = "",
      this.header = false,
      this.rowDelim = CsvRowDelim.Both,
      this.allowRowDelimInQuotes = true]) {
    escapedQuote = quoteEscape + quote;
  }

  @override
  String toString() {
    return 'CsvFormat(separator = "$separator", quote = "$quote", '
        'quoteEscape = "$quoteEscape", empty = "$empty", '
        'invalid = "$invalid", header = $header, rowDelim = $rowDelim, '
        'allowRowDelimInQuotes = $allowRowDelimInQuotes)';
  }

  /// Replaces all instances of `\r\n` with `\n`, then escapes all quotes and
  /// wraps the string in quotes.
  String escape(String text) {
    var text0 = text.replace("\r\n", "\n").replace(quote, escapedQuote);
    return "${quote}$text0${quote}";
  }

  /// Renders a single cell of data, escaping the value if necessary.
  String render(String text) {
    if (text.contains('\n') ||
        text.contains(separator) ||
        text.contains(quote)) {
      return escape(text);
    } else {
      return text;
    }
  }

  CsvFormat withSeparator(String separator) => copy(separator: separator);
  CsvFormat withQuote(String quote) => copy(quote: quote);
  CsvFormat withQuoteEscape(String quoteEscape) =>
      copy(quoteEscape: quoteEscape);
  CsvFormat withEmpty(String empty) => copy(empty: empty);
  CsvFormat withInvalid(String invalid) => copy(invalid: invalid);
  CsvFormat withHeader(bool header) => copy(header: header);
  CsvFormat withRowCsvDelim(CsvRowDelim rowDelim) => copy(rowDelim: rowDelim);
  CsvFormat withRowDelim(String rowDelim) =>
      copy(rowDelim: new CsvRowDelim(rowDelim));

  static final CSV = new CsvFormat(",");
  static final TSV = new CsvFormat("\t");

  static final Guess = new Partial(header: new Some(false));
}

class Partial extends GuessCsvFormat {
  final Option<String> separator;
  final Option<String> quote;
  final Option<String> quoteEscape;
  final Option<String> empty;
  final Option<String> invalid;
  final Option<bool> header;
  final Option<CsvRowDelim> rowDelim;
  final bool allowRowDelimInQuotes;

  Partial(
      [separator = const None(),
      quote = const None(),
      quoteEscape = const None(),
      empty = const None(),
      invalid = const None(),
      header = const None(),
      rowDelim = const None(),
      allowRowDelimInQuotes = true]);

  Partial withSeparator(String separator) =>
      copy(separator: new Some(separator));
  Partial withQuote(String quote) => copy(quote: new Some(quote));
  Partial withQuoteEscape(String quoteEscape) =>
      copy(quoteEscape: new Some(quoteEscape));
  Partial withEmpty(String empty) => copy(empty: new Some(empty));
  Partial withInvalid(String invalid) => copy(invalid: new Some(invalid));
  Partial withHeader(bool header) => copy(header: new Some(header));
  Partial withCsvRowDelim(CsvRowDelim rowDelim) =>
      copy(rowDelim: new Some(rowDelim));
  Partial withRowDelim(String rowDelim) =>
      copy(rowDelim: new Some(new CsvRowDelim(rowDelim)));

  /// Performs a very naive guess of the CsvFormat. This uses weighted
  /// frequencies of occurences of common separators, row-delimiters, quotes,
  /// quote escapes, etc. and simply selects the max for each. For empty
  /// values, it uses the frequency of the the possible empty values within
  /// the cells.
  ///
  /// This supports:
  ///
  ///  * \r\n and \n as row delimiters,
  ///  * ',', '\t', ';', and '|' as field delimiters,
  ///  * '"', and ''' as quote delimiter,
  ///  * the quote delimiter or \ for quote escapes,
  ///  * '', '?', '-', 'N/A', and 'NA' as empty values, and
  ///  * 'N/M' and 'NM' as invalid values.
  ///
  /// Headers are guessed by using the cosine similarity of the frequency of
  /// characters (except quotes/field delimiters) between the first row and
  /// all subsequent rows. Values below 0.5 will result in a header being
  /// inferred.
  CsvFormat call(String str) {
    int count(String ndl) {
      bool check(int i, [int j = 0]) {
        if (j >= ndl.length) {
          return true;
        } else if (i < str.length && str.charAt(i) == ndl.charAt(j)) {
          return check(i + 1, j + 1);
        } else {
          return false;
        }
      }

      int loop(int i, int cnt) {
        if (i < str.length) {
          return loop(i + 1, check(i) ? cnt + 1 : cnt);
        } else {
          return cnt;
        }
      }

      return loop(0, 0);
    }

    String choose(Map<String, double> weightedOptions, int f(String s)) {
      var weights = weightedOptions; //new Map(weightedOptions);
      var best, weight = weights.maxBy((c, w) => w * f(c));
      return weight > 0 ? best : weights.maxBy(_._2).v1;
    }

    var rowDelim0 = rowDelim.getOrElse(() {
      var windCnt = count("\r\n");
      var unixCnt = count("\n");

      if ((windCnt < 4 * unixCnt) && (unixCnt < 4 * windCnt)) {
        return CsvRowDelim.Both;
      } else if (windCnt < 4 * unixCnt) {
        return CsvRowDelim.Unix;
      } else {
        return CsvRowDelim.Windows;
      }
    });
    var separator0 = separator.getOrElse(() {
      return choose({",": 2.0, "\t": 3.0, ";": 2.0, "|": 1.0}, count);
    });
    var quote0 = quote.getOrElse(choose({"\"": 1.2, "\'": 1}, count));
    var quoteEscape0 = choose({"$quote0$quote0": 1.1, "\\$quote0": 1}, count)
        .dropRight(quote0.length);

//    val cells = for {
//      row0 <- str.split(Pattern.quote(rowDelim0.value))
//      row <- rowDelim0.alternate.fold(Array(row0)) { alt =>
//          row0.split(Pattern.quote(alt))
//        }
//      cell <- row.split(Pattern.quote(separator0))
//    } yield cell
    int matches(String value) => cells.filter((c) => c == value).size;
    var empty0 = empty.getOrElse(() {
      return choose({"": 3, "?": 2, "-": 2, "N/A": 1, "NA": 1}, matches);
    });
    var invalid0 = invalid.getOrElse(() {
      return matches("N/M") > 1 ? "N/M" : empty0;
    });

    val header0 =
        header.getOrElse(hasHeader(str, rowDelim0.value, separator0, quote0));

    return CsvFormat(separator0, quote0, quoteEscape0, empty0, invalid0,
        header0, rowDelim0, allowRowDelimInQuotes);
  }

  bool hasHeader(
      String chunk, String rowDelim, String separator, String quote) {
    Map<Char, double> mkVec(String s) => s
        .groupBy((c) => c)
        .map((k, v) => new Tuple2(k, v.length.toDouble()))
        .normalize();

    double similarity(Map<dynamic, double> x, Map<dynamic, double> y) =>
        x.dot(y) / (x.norm * y.norm);

    val headerEnd = chunk.indexOf(rowDelim);
    if (headerEnd > 0) {
      var hdr,
          rows =
          chunk.replace(separator, "").replace(quote, "").splitAt(headerEnd);
      print("header = ${similarity(mkVec(hdr), mkVec(rows))}");
      return similarity(mkVec(hdr), mkVec(rows)) < 0.5;
    } else {
      return false;
    }
  }
}
