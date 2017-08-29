package de.haw.PrepareHelper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * reverse edges => srcID < trgID
 */
public class EdgeNormalizer implements FlatMapFunction<Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>> {

  private static final long serialVersionUID = -2003859045309151399L;

  @Override
  public void flatMap(Tuple3<Double, Double, Double> value, Collector<Tuple3<Double, Double, Double>> out) throws Exception {

    if(value.f0 > value.f1) {
      out.collect(Tuple3.of(value.f1, value.f0, value.f2));
    } else {
      out.collect(value);
    }
  }
}
