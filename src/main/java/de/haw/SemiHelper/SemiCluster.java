package de.haw.SemiHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.graph.Edge;

public class SemiCluster implements Comparable<SemiCluster>, Serializable {

  private static final long serialVersionUID = 4665238766049346206L;

  private Map<Double, Set<Edge<Double, Double>>> vertexMap;    // Knoten-IDs im Cluster

  private Double score;                // entspricht S der Formel
  private Double weightInnerEdges;    // entspricht I_c der Formel
  private Double weightBoundedEdges;  // entspricht B_c der Formel
  private Double factor;                // entspricht F_b der Formel

  public SemiCluster(SemiParams params) {
    this.factor = params.getFactor();
    this.score = params.getInitScore();
    this.vertexMap = new HashMap<>();
    this.weightBoundedEdges = this.weightInnerEdges = 0.0;
  }

  public void addVertex(Double vid, Set<Edge<Double, Double>> edgeSet) {
    vertexMap.put(vid, edgeSet);
    for (Edge<Double, Double> edge : edgeSet) {
      /** beide Knoten befinden sich innerhalb des Clusters **/
      if (vertexMap.containsKey(edge.getSource()) && vertexMap.containsKey(edge.getTarget())) {
        weightInnerEdges += edge.getValue();    // beide Knoten liegen jetzt innerhalb des Clusters
        weightBoundedEdges -= edge.getValue();    // Kante verliess vorher das Cluster und wird nun subtrahiert
        /** lediglich ein Knoten befindet sich innerhalb des Cluster **/
      } else if (vertexMap.containsKey(edge.getSource()) || vertexMap.containsKey(edge.getTarget())) {
        weightBoundedEdges += edge.getValue();
      }
    }
  }

  void calculateScore() {
    /**
     * 		(I_c - f_b * B_c)
     * S =	 _________________
     * 		(V_c * (V_c - 1) *0.5)
     */
    if (vertexMap.size() == 1) {
      score = 1.0;
    } else {
      Double score_zaehler = weightInnerEdges - (factor * weightBoundedEdges);
      Double score_nenner = 0.5 * (vertexMap.size() * (vertexMap.size() - 1.0));
      score = score_zaehler / score_nenner;
    }
  }

  /**
   * @return Map mit allen Knoten im Cluster
   */
  public Map<Double, Set<Edge<Double, Double>>> getVertexMap() {
    return vertexMap;
  }

  /**
   * @return Score des Clusters (wird in dem Moment erst berechnet)
   */
  public Double getSemiScore() {
    return score;
  }

  @Override
  public String toString() {
    return "SemiCluster: " + vertexMap.keySet();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof SemiCluster)) {
      return false;
    }
    SemiCluster semiCluster = (SemiCluster) o;

    return new EqualsBuilder()
        .append(score, semiCluster.score)
        .append(vertexMap, semiCluster.vertexMap)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(score)
        .append(vertexMap)
        .toHashCode();
  }

  @Override
  public int compareTo(@SuppressWarnings("NullableProblems") SemiCluster other) {
    if (this.equals(other)) {
      return 0;
    }
    return this.score > other.score ? -1 : 1;
  }
}