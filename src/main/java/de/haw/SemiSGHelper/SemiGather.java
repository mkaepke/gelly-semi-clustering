package de.haw.SemiSGHelper;

import de.haw.SemiHelper.SemiCluster;
import de.haw.SemiHelper.SemiParams;
import de.haw.SemiHelper.SemiVertexValue;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

/**
 * UDF gather function (collect all incoming messages and set new vertex value)
 */
public class SemiGather extends GatherFunction<Double, SemiVertexValue, TreeSet<SemiCluster>> {

  private SemiParams semiParams;

  public SemiGather(SemiParams semiParams) {
    this.semiParams = semiParams;
  }

  private static final long serialVersionUID = -3200532278745971642L;

  @Override
  public void updateVertex(Vertex<Double, SemiVertexValue> vertex, MessageIterator<TreeSet<SemiCluster>> inMessages)
      throws Exception {

    boolean changed = false;
    TreeSet<SemiCluster> unionedClusterSet = new TreeSet<>();
    unionedClusterSet.clear();

    /**
     * Jeder Knoten iteriert ueber eingehende Cluster c1 ... ck
     * Wenn v noch nicht Bestandteil eines Clusters c ist, wird ein neues Cluster c' erzeugt und c um v erweitert
     */
    for (TreeSet<SemiCluster> clusterSet : inMessages) {    // Iteration ueber alle eingegangenen Nachrichten

      unionedClusterSet.addAll(clusterSet);

      for (SemiCluster semiCluster : clusterSet) {    // Iteration ueber alle Cluster von den benachbarten Knoten

        boolean contains = semiCluster.getVertexMap().containsKey(vertex.getId());

        /** wenn Knoten v im Cluster noch nicht enthalten und max. Clustergroesse noch nicht erreicht **/
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

    /**
     * Alle Cluster c1 , ..., ck werden nach Wert aufsteigend sortiert.
     * (hier: Knoten werden beim Hinzufuegen in das TreeSet automatisch sortiert)
     */

    /**
     * Jeder Knoten v aktualisiert seine Liste von Clustern zu denen aus c1,...,ck, die v beinhalten.
     */
    if (changed) {    // Vertex wird nur aktualisiert, falls neue SemiCluster hinzugekommen sind
      SemiVertexValue updatedCustomVertexValue = new SemiVertexValue(new TreeSet<>(), vertex.getValue().getIncidentEdgeSet(), semiParams);
      updatedCustomVertexValue.addBulkSemiClustersAndCalc(unionedClusterSet);

      setNewVertexValue(updatedCustomVertexValue);
    }
  }
}
