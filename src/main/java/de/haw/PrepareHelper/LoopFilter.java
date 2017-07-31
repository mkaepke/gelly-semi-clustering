package de.haw.PrepareHelper;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * remove self loops
 */
public class LoopFilter implements FilterFunction<Tuple3<Double, Double, Double>> {

  private static final long serialVersionUID = -2823984798289416478L;

  @Override
  public boolean filter(Tuple3<Double, Double, Double> value) throws Exception {
    return !value.f0.equals(value.f1);
  }
}
