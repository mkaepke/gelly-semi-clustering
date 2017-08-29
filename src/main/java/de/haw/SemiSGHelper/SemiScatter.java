package de.haw.SemiSGHelper;

import de.haw.SemiHelper.SemiCluster;
import de.haw.SemiHelper.SemiVertexValue;
import java.util.TreeSet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

/**
 * UDF scatter function (send message to all neighbours)
 */
public class SemiScatter extends ScatterFunction<Double, SemiVertexValue, TreeSet<SemiCluster>, Double> {

  private static final long serialVersionUID = 6462382132746205753L;

  @Override
  public void sendMessages(Vertex<Double, SemiVertexValue> vertex) throws Exception {

    sendMessageToAllNeighbors(vertex.getValue().getSemiClusterSet());
  }
}
