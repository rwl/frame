import 'package:frame/frame.dart';

class Company {
  Company(String name, String exchange, String ticker, String currency,
      BigDecimal marketCap);
}

main() {
  var Acme = new Company("Acme", "NASDAQ", "ACME", "USD", BigDecimal("123.00"));
  var BobCo =
      new Company("Bob Company", "NASDAQ", "BOB", "USD", BigDecimal("45.67"));
  var Cruddy =
      new Company("Cruddy Inc", "XETRA", "CRUD", "EUR", BigDecimal("1.00"));
  var res0 = new Frame.fromRows(Acme, BobCo, Cruddy);
  print(res0.toString());

  var res1 = res0.mapColKeys(
      {0: "Name", 1: "Exchange", 2: "Ticker", 3: "Currency", 4: "Market Cap"});

  var ticker = new Cols("Ticker").as[String];
  var frame = res1.reindex(ticker);

  var res2 = frame[BigDecimal]("ACME", "Market Cap");
  frame[Double]("BOB", "Market Cap");
  frame[LocalDate]("CRUD", "Market Cap");

  var marketCap = new Cols("Market Cap").as[BigDecimal];
  frame.get(marketCap);

  var ci = fetchCompanyInfo("GM", "HMC", "BMW.DE");
  var companies = ci.reindex(ticker);
}

/// Parses a value with an optional "scale" suffix at the end, such as 123K,
/// 24M, or 3.12B. This will return the scaled `BigDecimal`. Valid scales are
/// currently K, M, B, or T, for 1,000, 1,000,000, 1,000,000,000, and
/// 1,000,000,000,000 respectively.
Cell<BigDecimal> parseScaledAmount(String value) {
  if (value.toUpperCase() == ScaledAmount(value, scale)) {
    return new Value(BigDecimal(value) * getScale(scale));
  } else {
    return NM;
  }
}

/// Fetch some basic company info for the given IDs. This returns a frame with
/// the following columns: Name, Stock Exchange, Ticker, Currency, and Market
/// Cap. There is 1 row per company.
Frame<int, String> fetchCompanyInfo(Iterable<String> ids) {
  var idList = ids.mkString(",");
  var fields = "n0x0s0c4j1";
  var csv = fetchCsv(
      "http://download.finance.yahoo.com/d/quotes.csv?s=$idList&f=$fields&e=.csv");
  return csv.unlabeled.toFrame().withColIndex(new Index.fromKeys(
      "Name", "Stock Exchange", "Ticker", "Currency", "Market Cap"));
}

/// Fetches the last 5 years of share price data, given a id, from Yahoo!
/// Finance.
Frame<int, String> fetchSharePriceData(String id) =>
    fetchSharePriceData(id, LocalDate.now().minusYears(5), LocalDate.now());

Frame<int, String> fetchSharePriceData2(String id1, String id2, String id3) {
  var ids = [id1, id2, id3];
  ids.map((id) {
    var frame = fetchSharePriceData(id);
    var names = new Series(frame.rowIndex, Column.value(id));
    return frame.join("Ticker", names, Join.Inner);
  }).reduceLeft((a, b) => a.appendRows(b));
}

/// Fetches some historical share price data from Yahoo! Finance. The result
/// is returned as a CSV file, so we use Framian's CSV parser to convert it to
/// a frame.
Frame<int, String> fetchSharePriceData3(
    String id, LocalDate start, LocalDate end) {
  var paramMap = {
    "s": id,
    "a": (start.getMonthOfYear - 1),
    "b": start.getDayOfMonth,
    "c": start.getYear,
    "d": (end.getMonthOfYear - 1),
    "e": end.getDayOfMonth,
    "f": end.getYear,
    "ignore": ".csv"
  };
  var queryString = paramMap.map((k, v) => "$k=$v").mkString("&");
  var csv =
      fetchCsv("http://real-chart.finance.yahoo.com/table.csv?$queryString");
  return csv.labeled.toFrame();
}

/// Fetches the exchange rate data from Yahoo! Finance. All rates are for
/// conversions to USD.
Frame<int, String> fetchExchangeRate(Iterable<String> currencies) {
  var ids = currencies.map((c) => c + "USD=X").mkString(",");
  var fields = "c4l1";
  var csv = fetchCsv(
      "http://download.finance.yahoo.com/d/quotes.csv?s=$ids&f=$fields&e=.csv");
  return csv.unlabeled
      .toFrame()
      .withColIndex(Index.fromKeys("Currency", "Rate"));
}

Csv fetchCsv(String url) {
  var csv = scala.io.Source.fromURL(url).mkString();
  return new Csv.parseString(csv);
}

var ScaledAmount = new RegExp(r"(\d+(?:\.\d+))([kKmMbB])?");

BigDecimal getScale(String suffix) {
  switch (suffix.toUpperCase()) {
    case "K":
      return BigDecimal(1000);
    case "M":
      return BigDecimal(1000000);
    case "B":
      return BigDecimal(1000000000);
    case "T":
      return BigDecimal("1000000000000");
  }
}
