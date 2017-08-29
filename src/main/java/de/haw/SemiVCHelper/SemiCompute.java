package de.haw.SemiVCHelper;

import de.haw.SemiHelper.SemiCluster;
import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;

/**
 * This method is invoked once per superstep, for each active vertex.
 * A vertex is active during a superstep, if at least one message was produced for it,
 * in the previous superstep. During the first superstep, all vertices are active.
 * This method can iterate over all received messages, set the new vertex value, and
 * send messages to other vertices (which will be delivered in the next superstep).
 **/
public class SemiCompute extends ComputeFunction<Double, SemiVertexValue, Double, TreeSet<SemiCluster>> {

  private static final long serialVersionUID = -8639053068066676353L;

  private SemiParams semiParams;

  public SemiCompute(SemiParams semiParams) {
    this.semiParams = semiParams;
  }

  @Override
  public void compute(Vertex<Double, SemiVertexValue> vertex, MessageIterator<TreeSet<SemiCluster>> inMessages)
      throws Exception {

    boolean changed = false;
    TreeSet<SemiCluster> unionedClusterSet = new TreeSet<>();
    unionedClusterSet.clear();

    for (TreeSet<SemiCluster> semiClusterSet : inMessages) {
      unionedClusterSet.addAll(semiClusterSet);

      for (SemiCluster semiCluster : semiClusterSet) {
        boolean contains = semiCluster.getVertexMap().containsKey(vertex.getId());
        if (!contains && semiCluster.getVertexMap().size() < semiParams.getClusterCapacity()) {
          SemiCluster newSemiCluster = new SemiCluster(semiParams);
          for (Map.Entry<Double, Set<Edge<Double, Double>>> entrySet : semiCluster.getVertexMap().entrySet()) {
            newSemiCluster.addVertex(entrySet.getKey(), entrySet.getValue());
          }
          newSemiCluster.addVertex(vertex.getId(), vertex.getValue().getIncidentEdgeSet());
          unionedClusterSet.add(newSemiCluster);

          changed = true;
        } else if (contains) {
          if (!unionedClusterSet.contains(semiCluster)) {
            unionedClusterSet.add(semiCluster);
            changed = true;
          }
        }
      }
    }
    if (changed) {    // Vertex wird nur aktualisiert, falls neue SemiCluster hinzugekommen sind
      SemiVertexValue updatedCustomVertexValue = new SemiVertexValue(new TreeSet<>(), vertex.getValue().getIncidentEdgeSet(), semiParams);
      updatedCustomVertexValue.addBulkSemiClustersAndCalc(unionedClusterSet);

      setNewVertexValue(updatedCustomVertexValue);
    }
    sendMessageToAllNeighbors(vertex.getValue().getSemiClusterSet());
  }
}
