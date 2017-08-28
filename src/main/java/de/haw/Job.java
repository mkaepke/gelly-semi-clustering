package de.haw;

import de.haw.PrepareHelper.EdgeNormalizer;
import de.haw.PrepareHelper.LoopFilter;
import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class Job {

  public static void main(String[] args) throws Exception {

    /**
     * parse args
     */
    ParameterTool parameter = ParameterTool.fromArgs(args);

    /**
     * set values for SemiClustering by arguments or keep default
     */
    SemiParams semiParams = new SemiParams();
    semiParams.setMaxIterations(parameter.getInt("m", 2));
    semiParams.setInitScore(parameter.getDouble("i", 1.0));
    semiParams.setFactor(parameter.getDouble("f", 0.5));
    semiParams.setTopXOfClusters(parameter.getInt("x", 3));
    semiParams.setClusterCapacity(parameter.getInt("c", 2));

    /**
     * set up the execution environment
     */
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parameter.getInt("p", 4));
    env.getConfig().enableObjectReuse();
    env.getConfig().disableSysoutLogging();

    /**
     * read edge file
     */
    DataSet<Tuple3<Double, Double, Double>> edgeTuples = env.readCsvFile(parameter.getRequired("input"))
        .fieldDelimiter(",")
        .ignoreComments("%")
        .types(Double.class, Double.class, Double.class);

    /**
     * prepare input
     */
    DataSet<Tuple3<Double, Double, Double>> preparedEdgeTuple = edgeTuples
        .filter(new LoopFilter())
        .name("remove self loops")
        .flatMap(new EdgeNormalizer())
        .name("reverse edges => srcID < trgID")
        .groupBy(0, 1)
        .sum(2)
        .name("combine multi edges");

    /**
     * build input graph
     */
    Graph<Double, NullValue, Double> inputGraph = Graph.fromTupleDataSet(preparedEdgeTuple, env);

    /**
     * run SemiClustering
     */
    Graph<Double, SemiVertexValue, Double> result;
    switch(parameter.getRequired("algo")) {
      case "sg" : result = inputGraph.run(new SemiClusteringSG(semiParams, env)); break;
      case "vc" : result = inputGraph.run(new SemiClusteringPregel(semiParams, env)); break;

      default : throw new IllegalArgumentException("choose: sg for scattergather or vc for vertex-centric/pregel");
    }
    System.err.println("num of vertices: " + result.numberOfVertices());
  }
}
