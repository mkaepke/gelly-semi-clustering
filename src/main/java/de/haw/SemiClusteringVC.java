package de.haw;

import de.haw.PrepareHelper.EdgeNormalizer;
import de.haw.PrepareHelper.IncidentEdges;
import de.haw.PrepareHelper.LoopFilter;
import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import de.haw.SemiVCHelper.SemiCombiner;
import de.haw.SemiVCHelper.SemiCompute;
import java.io.Serializable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

public class SemiClusteringVC implements GraphAlgorithm<Double, NullValue, Double, Graph<Double, SemiVertexValue, Double>>,
    Serializable {

  private static final long serialVersionUID = 6514847000346414546L;

  private SemiParams semiParams;
  private ExecutionEnvironment env;

  public SemiClusteringVC(ParameterTool parameter, ExecutionEnvironment env) {
    this.semiParams = new SemiParams();
    this.env = env;

    /**
     * set values for Semi-Clustering by arguments or keep default
     */
    semiParams.setMaxIterations(parameter.getInt("m", 2));
    semiParams.setInitScore(parameter.getDouble("i", 1.0));
    semiParams.setFactor(parameter.getDouble("f", 0.5));
    semiParams.setTopXOfClusters(parameter.getInt("x", 3));
    semiParams.setClusterCapacity(parameter.getInt("c", 2));
  }

  @Override
  public Graph<Double, SemiVertexValue, Double> run(Graph<Double, NullValue, Double> input) throws Exception {
    Preconditions.checkArgument(semiParams.getMaxIterations() > 0, "max iteration must be greater than 0");

    /**
     * prepare input and optimize edges
     */
    DataSet<Tuple3<Double, Double, Double>> preparedEdgeTuple = input.getEdgesAsTuple3()
        .filter(new LoopFilter())
        .name("remove self loops")
        .flatMap(new EdgeNormalizer())
        .name("reverse edges => srcID < trgID")
        .groupBy(0, 1)
        .sum(2)
        .name("combine multi edges");
    input = Graph.fromTupleDataSet(preparedEdgeTuple, env);

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
