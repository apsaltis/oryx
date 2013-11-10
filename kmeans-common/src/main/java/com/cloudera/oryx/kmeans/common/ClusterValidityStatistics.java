/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.kmeans.common;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixPreservingVisitor;

import java.io.Serializable;

import com.cloudera.oryx.common.io.DelimitedDataUtils;

public final class ClusterValidityStatistics implements Serializable {

  private final int k;
  private final int replica;
  private final double testCost;
  private final double trainCost;
  private final double vanDongen;
  private final double varInfo;

  public static ClusterValidityStatistics create(Iterable<WeightedRealVector> points,
                                                 Centers test,
                                                 Centers train,
                                                 int replicaId) {
    Preconditions.checkArgument(test.size() == train.size());
    int k = test.size();
    double n = 0.0;
    double testCost = 0.0;
    double trainCost = 0.0;
    RealMatrix contingencyMatrix = new Array2DRowRealMatrix(k, k);
    double[] rowSums = new double[k];
    double[] colSums = new double[k];
    for (WeightedRealVector wv : points) {
      Distance d1 = test.getDistance(wv.thing());
      Distance d2 = train.getDistance(wv.thing());
      contingencyMatrix.addToEntry(d1.getClosestCenterId(), d2.getClosestCenterId(), wv.weight());
      rowSums[d1.getClosestCenterId()] += wv.weight();
      colSums[d2.getClosestCenterId()] += wv.weight();
      testCost += d1.getSquaredDistance() * wv.weight();
      trainCost += d2.getSquaredDistance() * wv.weight();
      n += wv.weight();
    }

    return new ClusterValidityStatistics(k, replicaId, testCost, trainCost,
        normVarInformation(contingencyMatrix, rowSums, colSums, n),
        normVanDongen(contingencyMatrix, rowSums, colSums, n));
  }

  public static ClusterValidityStatistics parse(String str) {
    String[] pieces = DelimitedDataUtils.decode(str);
    Preconditions.checkArgument(pieces.length == 6, "Invalid delimited string: " + str);
    return new ClusterValidityStatistics(
        Integer.valueOf(pieces[0]),
        Integer.valueOf(pieces[1]),
        Double.valueOf(pieces[2]),
        Double.valueOf(pieces[3]),
        Double.valueOf(pieces[4]),
        Double.valueOf(pieces[5]));
  }

  private ClusterValidityStatistics(int k, int replica, double testCost, double trainCost,
                                    double varInfo, double vanDongen) {
    this.k = k;
    this.replica = replica;
    this.testCost = testCost;
    this.trainCost = trainCost;
    this.varInfo = varInfo;
    this.vanDongen = vanDongen;
  }

  @Override
  public String toString() {
    return DelimitedDataUtils.encode(k, replica, testCost, trainCost, varInfo, vanDongen);
  }

  public int getK() {
    return k;
  }

  public int getReplica() {
    return replica;
  }

  public double getTotalCost() {
    return testCost + trainCost;
  }

  public double getTestCost() {
    return testCost;
  }

  public double getTrainCost() {
    return trainCost;
  }

  public double getVanDongen() {
    return vanDongen;
  }

  public double getVariationOfInformation() {
    return varInfo;
  }

  /**
   * Calculates the normalized van Dongen criterion for the contingency contingencyMatrix.
   *
   * @return the normalized van Dongen criterion for the contingency contingencyMatrix
   */
  private static double normVanDongen(RealMatrix contingencyMatrix, double[] rowSums, double[] colSums, double n) {
    double rs = 0.0;
    double cs = 0.0;
    double rmax = 0.0;
    double cmax = 0.0;
    for (int i = 0; i < rowSums.length; i++) {
      rs += contingencyMatrix.getRowVector(i).getLInfNorm();
      cs += contingencyMatrix.getColumnVector(i).getLInfNorm();
      rmax = Math.max(rmax, rowSums[i]);
      cmax = Math.max(cmax, colSums[i]);
    }
    double den = 2 * n - rmax - cmax;
    if (den == 0.0) {
      return Double.NaN;
    }
    return (2 * n - rs - cs) / den;
  }

  /**
   * Calculates the normalized variation-of-information for the contingency contingencyMatrix.
   *
   * @return the normalized variation-of-information for the contingency contingencyMatrix
   */
  private static double normVarInformation(
      RealMatrix contingencyMatrix,
      final double[] rowSums,
      final double[] colSums,
      final double n) {
    double den = n * (entropy(rowSums, n) + entropy(colSums, n));
    if (den == 0) {
      return Double.NaN;
    }

    double num = contingencyMatrix.walkInOptimizedOrder(new RealMatrixPreservingVisitor() {
      private double sum = 0.0;
      @Override
      public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {
        sum = 0.0;
      }

      @Override
      public void visit(int row, int column, double value) {
        if (value > 0.0) {
          sum += value * (Math.log(value * n) - Math.log(rowSums[row]) - Math.log(colSums[column]));
        }
      }

      @Override
      public double end() {
        return sum;
      }
    });

    return 1.0 + 2.0 * (num / den);
  }

  private static double entropy(double[] sums, double n) {
    double e = 0.0;
    double logn = Math.log(n);
    for (double sum : sums) {
      if (sum > 0.0) {
        e += sum * (Math.log(sum) - logn);
      }
    }
    return e / n;
  }
}
