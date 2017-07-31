package de.haw.PrepareHelper;

import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * create a new vertex value with all incident edges and the intial Semi-Cluster
 */
public class IncidentEdges implements EdgesFunctionWithVertexValue<Double, NullValue, Double, Vertex<Double, SemiVertexValue>> {

  private static final long serialVersionUID = 1834677868839443065L;

  private SemiParams semiParams;

  public IncidentEdges(SemiParams semiParams) {
    this.semiParams = semiParams;
  }

  @Override
  public void iterateEdges(Vertex<Double, NullValue> vertex, Iterable<Edge<Double, Double>> edges,
      Collector<Vertex<Double, SemiVertexValue>> out) throws Exception {

    Set<Edge<Double, Double>> edgeSet = new HashSet<>();

    SemiVertexValue semiVertexValue = new SemiVertexValue(semiParams);
    for(Edge<Double, Double> edge : edges) {
      edgeSet.add(edge);
    }

    semiVertexValue.addEdges(edgeSet);

    Vertex<Double, SemiVertexValue> newVertex = new Vertex<>(vertex.getId(), semiVertexValue);
    out.collect(newVertex);
  }
}
