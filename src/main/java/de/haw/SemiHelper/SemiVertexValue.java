package de.haw.SemiHelper;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.graph.Edge;

public class SemiVertexValue implements Serializable {

  private static final long serialVersionUID = -8314021582081942645L;

  private Set<Edge<Double, Double>> edgeSet;
  private TreeSet<SemiCluster> clusters;
  private SemiParams params;

  public SemiVertexValue(SemiParams params) {
    this.params = params;
    this.edgeSet = new HashSet<>();
    this.clusters = new TreeSet<>();
  }

  public SemiVertexValue(TreeSet<SemiCluster> val, Set<Edge<Double, Double>> edgeSet, SemiParams params) {
    this.clusters = val;
    this.edgeSet = edgeSet;
    this.params = params;
  }

  public void addEdges(Set<Edge<Double, Double>> edgeSet) {
    this.edgeSet.addAll(edgeSet);
  }

  /**
   * Gibt alle inzidenten Kanten zurueck
   */
  public Set<Edge<Double, Double>> getIncidentEdgeSet() {
    return edgeSet;
  }

  public TreeSet<SemiCluster> getSemiClusterSet() {
    return clusters;
  }

  public void addBulkSemiClustersAndCalc(TreeSet<SemiCluster> semiClusterSet) {
    clusters.addAll(semiClusterSet);
    calculateSemiClusters();
    if (clusters.size() > params.getTopXOfClusters()) {
      while (clusters.size() > params.getTopXOfClusters()) {
        clusters.pollLast();
      }
    }
  }

  private void calculateSemiClusters() {
    TreeSet<SemiCluster> updatedScore = new TreeSet<>();
    for (SemiCluster semiCluster : clusters) {
      semiCluster.calculateScore();
      updatedScore.add(semiCluster);
    }
    clusters.clear();
    clusters.addAll(updatedScore);
  }

  @Override
  public String toString() {
    return "EdgeSet: " + getIncidentEdgeSet().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof SemiVertexValue)) {
      return false;
    }
    SemiVertexValue customVertexValue = (SemiVertexValue) o;

    return new EqualsBuilder()
        .append(edgeSet, customVertexValue.edgeSet)
        .append(clusters, customVertexValue.clusters)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(edgeSet)
        .append(clusters)
        .toHashCode();
  }
}
