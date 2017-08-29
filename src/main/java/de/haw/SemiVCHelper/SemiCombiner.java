package de.haw.SemiVCHelper;

import de.haw.SemiHelper.SemiCluster;
import java.util.TreeSet;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

/**
 * Combines messages sent from different vertices to a target vertex.
 * Implementing this method might reduce communication costs during a vertex-centric
 * iteration.
 *
 * not necessary for Semi-Clustering
 */
public class SemiCombiner extends MessageCombiner<Double, TreeSet<SemiCluster>> {

  private static final long serialVersionUID = 3252260097782050509L;

  @Override
  public void combineMessages(MessageIterator<TreeSet<SemiCluster>> inMessages) throws Exception {
    // not in use
  }
}
