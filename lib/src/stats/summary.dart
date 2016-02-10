library frame.stats.summary;

class Summary {
  String Mean = "Mean";
  String Median = "Median";
  String Max = "Max";
  String Min = "Min";

  // TODO: Provide way to choose "optimal" field type for frame row/col.

  Frame<Col, String> frame(Frame<Row, Col> f) {
    //import f.{ colClassTag, colOrder }

    return new Frame.mergeColumns([
      new Tuple2(Mean, f.reduceFrame(reduce.Mean[Number])),
      new Tuple2(Median, f.reduceFrame(reduce.Median[Number])),
      new Tuple2(Max, f.reduceFrame(reduce.Max[Number])),
      new Tuple2(Min, f.reduceFrame(reduce.Min[Number]))
    ]);
  }

  Series<String, V> series(Series<K, V> s) {
    return new Series.fromCells([
      new Tuple2(Mean, s.reduce(reduce.Mean[V])),
      new Tuple2(Median, s.reduce(reduce.Median[V])),
      new Tuple2(Max, s.reduce(reduce.Max[V])),
      new Tuple2(Min, s.reduce(reduce.Min[V]))
    ]);
  }
}
