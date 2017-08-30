package de.haw;

import de.haw.SemiHelper.SemiVertexValue;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * Example job for running the Semi-Clustering algorithm by a comma separated edge-list as input file
 *
 * e.g.: --input /Users/path/to/your/edge/list.txt --algo vc
 *
 * optional parameter:
 * --m for max iteration (def.: 2)
 * --i to set init score (def.: 1.0)
 * --f to set score factor (def.: 0.5)
 * --x for top X clusters (bests) (def.: 3)
 * --c for SemiCluster capacity (def.: 2)
 */
public class Job {

  public static void main(String[] args) throws Exception {

    /**
     * parse args
     */
    ParameterTool parameter = ParameterTool.fromArgs(args);

    /**
     * set up the execution environment
     */
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parameter.getInt("p", 1));
    env.getConfig().enableObjectReuse();

    /**
     * read edge file
     */
    DataSet<Tuple3<Double, Double, Double>> edgeTuples = env.readCsvFile(parameter.getRequired("input"))
        .fieldDelimiter(",")
        .ignoreComments("%")
        .types(Double.class, Double.class, Double.class);

    /**
     * build input graph
     */
    Graph<Double, NullValue, Double> inputGraph = Graph.fromTupleDataSet(edgeTuples, env);

    /**
     * run Semi-Clustering
     */
    Graph<Double, SemiVertexValue, Double> result;
    switch(parameter.getRequired("algo")) {
      case "sg" : result = inputGraph.run(new SemiClusteringSG(parameter, env)); break;
      case "vc" : result = inputGraph.run(new SemiClusteringVC(parameter, env)); break;

      default : throw new IllegalArgumentException("choose: sg for scatter-gather or vc for vertex-centric");
    }

    /**
     * print for lazy execution
     */
    System.err.println(result.numberOfVertices());
  }
}
