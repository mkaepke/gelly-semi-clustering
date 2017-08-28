package de.haw.SemiPregelHelper;

import de.haw.SemiHelper.SemiCluster;
import java.util.TreeSet;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

/**
 * Combines messages sent from different vertices to a target vertex.
 * Implementing this method might reduce communication costs during a vertex-centric
 * iteration.
 */
public class SemiCombiner extends MessageCombiner<Double, TreeSet<SemiCluster>> {

  private static final long serialVersionUID = 3252260097782050509L;

  @Override
  public void combineMessages(MessageIterator<TreeSet<SemiCluster>> inMessages) throws Exception {

    // combine Funktion wird nicht benötigt -> Faktor 10 schnellere Laufzeit
//    TreeSet<SemiCluster> result = new TreeSet<>();
//
//    for (TreeSet<SemiCluster> treeSet : inMessages) {
//      result.addAll(treeSet);
//    }
//
//    sendCombinedMessage(result);
  }
}
