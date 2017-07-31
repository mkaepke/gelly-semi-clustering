package de.haw;

import de.haw.PrepareHelper.IncidentEdges;
import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import de.haw.SemiSGHelper.SemiGather;
import de.haw.SemiSGHelper.SemiScatter;
import java.io.Serializable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

public class SemiClusteringSG implements GraphAlgorithm<Double, NullValue, Double, Graph<Double, SemiVertexValue, Double>>, Serializable {

  private static final long serialVersionUID = 8753438941080071962L;

  private SemiParams semiParams;
  private ExecutionEnvironment env;

  public SemiClusteringSG(SemiParams semiParams, ExecutionEnvironment env) {
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

    /**
     * run iteration
     */
    ScatterGatherConfiguration configuration = new ScatterGatherConfiguration();
    configuration.setDirection(EdgeDirection.ALL);

    return runnableGraph.runScatterGatherIteration(new SemiScatter(), new SemiGather(semiParams), semiParams.getMaxIterations(), configuration);
  }
}
