library frame.stats.ops;

/*implicit*/ class FrameStatsOps<Row, Col> {
  FrameStatsOps(Frame<Row, Col> self);

  Frame<Col, String> summary() => framian.stats.summary(self);
}

/*implicit*/ class SeriesStatsOps<K, V> {
  SeriesStatsOps(Series<K, V> self);

  Series<String, V> summary(
          /*implicit V0: Order[V], V1: Field[V], ct: ClassTag[V]*/) =>
      framian.stats.summary(self);
}
