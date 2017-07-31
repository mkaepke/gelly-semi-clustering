package de.haw;

import de.haw.PrepareHelper.IncidentEdges;
import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import de.haw.SemiPregelHelper.SemiCombiner;
import de.haw.SemiPregelHelper.SemiCompute;
import java.io.Serializable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

public class SemiClusteringPregel implements GraphAlgorithm<Double, NullValue, Double, Graph<Double, SemiVertexValue, Double>>,
    Serializable {

  private static final long serialVersionUID = 6514847000346414546L;

  private SemiParams semiParams;
  private ExecutionEnvironment env;

  public SemiClusteringPregel(SemiParams semiParams, ExecutionEnvironment env) {
    this.semiParams = semiParams;
    this.env = env;
  }

  @Override
  public Graph<Double, SemiVertexValue, Double> run(Graph<Double, NullValue, Double> input) throws Exception {
    Preconditions.checkArgument(semiParams.getMaxIterations() > 0, "max iteration must be greater than 0");

    /**
     * prepare graph
     */
    DataSet<Vertex<Double, SemiVertexValue>> vertexDataSet = input
        .groupReduceOnEdges(new IncidentEdges(semiParams), EdgeDirection.ALL);
    Graph<Double, SemiVertexValue, Double> runnableGraph = Graph.fromDataSet(vertexDataSet, input.getEdges(), env);
    runnableGraph = runnableGraph.getUndirected();

    /**
     * run iteration
     */
    return runnableGraph.runVertexCentricIteration(new SemiCompute(semiParams), new SemiCombiner(), semiParams.getMaxIterations());
  }
}
