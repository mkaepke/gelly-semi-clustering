package de.haw.SemiSGHelper;

import de.haw.SemiHelper.SemiCluster;
import de.haw.SemiHelper.SemiVertexValue;
import java.util.TreeSet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

/**
 * benutzerdefinierte Scatterfunktion (verschickt Nachrichten an benachbarte Knoten)
 * <p>
 * K - The type of the vertex key (the vertex identifier).
 * VV - The type of the vertex value (the state of the vertex).
 * Message - The type of the message sent between vertices along the edges.
 * EV - The type of the values that are associated with the edges.
 */
public class SemiScatter extends ScatterFunction<Double, SemiVertexValue, TreeSet<SemiCluster>, Double> {

  private static final long serialVersionUID = 6462382132746205753L;

  @Override
  public void sendMessages(Vertex<Double, SemiVertexValue> vertex) throws Exception {
    /**
     * jeder Knoten verschickt seine Clusterliste an alle seine Nachbarknoten
     */
    sendMessageToAllNeighbors(vertex.getValue().getSemiClusterSet());
  }
}
