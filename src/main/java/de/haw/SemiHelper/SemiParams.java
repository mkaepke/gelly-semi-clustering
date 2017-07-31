package de.haw.SemiHelper;

import java.io.Serializable;

public class SemiParams implements Serializable {

  private static final long serialVersionUID = -682514402747913528L;

  private Double factor;
  private static Integer topXOfClusters;
  private static Integer maxIterations;
  private static Integer clusterCapacity;

  public Double getInitScore() {
    return initScore;
  }

  public void setInitScore(Double initScore) {
    this.initScore = initScore;
  }

  private Double initScore;

  public Double getFactor() {
    return factor;
  }

  public void setFactor(Double factor) {
    this.factor = factor;
  }

  public Integer getTopXOfClusters() {
    return topXOfClusters;
  }

  public void setTopXOfClusters(Integer topXOfClusters) {
    SemiParams.topXOfClusters = topXOfClusters;
  }

  public Integer getMaxIterations() {
    return maxIterations;
  }

  public void setMaxIterations(Integer maxIterations) {
    SemiParams.maxIterations = maxIterations;
  }

  public Integer getClusterCapacity() {
    return clusterCapacity;
  }

  public void setClusterCapacity(Integer maxVerticesPerCluster) {
    SemiParams.clusterCapacity = maxVerticesPerCluster;
  }

}
