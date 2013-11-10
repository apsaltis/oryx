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

package com.cloudera.oryx.als.computation;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.cloudera.oryx.als.common.DataUtils;
import com.cloudera.oryx.als.common.pmml.ALSModelDescription;
import com.cloudera.oryx.als.computation.merge.MergeIDMappingStep;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.settings.ConfigUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.typesafe.config.Config;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.computation.initialy.InitialYStep;
import com.cloudera.oryx.als.computation.iterate.row.RowStep;
import com.cloudera.oryx.als.computation.known.CollectKnownItemsStep;
import com.cloudera.oryx.als.computation.merge.MergeNewOldStep;
import com.cloudera.oryx.als.computation.merge.ToItemVectorsStep;
import com.cloudera.oryx.als.computation.merge.ToUserVectorsStep;
import com.cloudera.oryx.als.computation.popular.PopularItemStep;
import com.cloudera.oryx.als.computation.popular.PopularUserStep;
import com.cloudera.oryx.als.computation.publish.PublishXStep;
import com.cloudera.oryx.als.computation.publish.PublishYStep;
import com.cloudera.oryx.als.computation.recommend.CollectRecommendStep;
import com.cloudera.oryx.als.computation.recommend.DistributeRecommendWorkStep;
import com.cloudera.oryx.als.computation.recommend.RecommendStep;
import com.cloudera.oryx.als.computation.similar.DistributeSimilarWorkStep;
import com.cloudera.oryx.als.computation.similar.SimilarStep;
import com.cloudera.oryx.common.math.MatrixUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.stats.DoubleWeightedMean;
import com.cloudera.oryx.computation.common.DependsOn;
import com.cloudera.oryx.computation.common.DistributedGenerationRunner;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;

/**
 * <p>This class will run one generation of the Computation Layer. It is essentially a client of Hadoop
 * and starts and monitors a series of MapReduce jobs that complete the Computation Layer's work.</p>
 *
 * <p>This is an overview of the sequence of MapReduce jobs that can run as part of the ALS pipeline.
 * While it is one flow, there are several distinct concerns inside of it, some optional. The core
 * flow is presented first, followed by notes about secondary or optional aspects of the flow. These
 * are presented in order of execution although in many cases some jobs run in parallel with others.</p>
 *
 * <p>In all cases, generation N's data is found at HDFS path {@code /[bucket]/[N]/} (where N is
 * padded to 5 digits so that lexical order equals numeric order). Locations are relative to this
 * unless otherwise specified.</p>
 *
 * <ul>
 * <li>"varlong" refers to the representation {@code long}</li>
 * <li>"varint" refers to {@code int}</li>
 * <li>"idvalue" refers to {@code IDValue}</li>
 * <li>"floatarray" refers to {@code float[]}</li>
 * <li>"floatmap" refers to {@code FastByIDFloatMap}</li>
 * <li>"idset" refers to {@code FastIDSet}</li>
 * </ul>
 *
 *
 * <h2>Core Steps</2>
 *
 * <h3>MergeNewOldStep</h3>
 *
 * <p>Merges all data from last generation with new input in the current generation. The last generation's
 * data exists in generation N-1 under {@code input/}. It encodes user,item,value tuples as
 * user-(item,value) key/value pairs, in particular as varlong mapped to idvalue objects.</p>
 *
 * <p>New input is always found in generation N under {@code inbound/}, in (compressed) CSV text files.
 * This step merges old and new values by adding them. Here is where 'decay' of old data takes place too.</p>
 *
 * <p>Output goes to generation N's {@code input/}, and this is what drives all further computation in
 * the generation.</p>
 *
 * <h3>ToUserVectorsStep / ToItemVectorsStep</h3>
 *
 * <p>These simply transform the output above into a mapping from user to all item/value pairs as a single
 * floatmap object -- user vectors if you like -- and likewise for item vectors. The output goes to
 * {@code userVectors/} and {@code itemVectors/} respectively.</p>
 *
 * <h3>CollectKnownItemsStep</h3>
 *
 * <p>Outputs user IDs mapped to a set of item IDs that the user ID has interacted with in the data set.
 * The output is in text format, in {@code knownItems/} and is for consumption by the Serving Layer.</p>
 *
 * <h3>InitialYStep</h3>
 *
 * <p>This process outputs an initial Y matrix. It will optionally reuse the final {code Y/} output from
 * last generation, N-1. For any items that have no vector yet (which might be all of them on a fresh run)
 * it will choose random unit vectors (chosen with kmeans++-style preference for dissimilar vectors).
 * Output goes to {@code tmp/iterations/0/Y}, and is encoded as varlong mapped to floatarray.</p>
 *
 * <h3>PopularUserStep / PopularItemStep</h3>
 *
 * <p>Slightly misnamed, as they actually output the set of items that a given user interacts with in the
 * data, and vice versa for items. The output is encoded as varlong mapped to idset, and the output goes
 * to {@code popularUsersByItemPartition} and {@code popularItemsByUserPartition} respectively.. This is
 * used later to cleverly side-load only the part of the X or Y matrix that will actually be used by
 * a reducer.</p>
 *
 * <h3>RowStep</h3>
 *
 * <p>The heart of the ALS iteration, and the most important step in the flow. This is the one that is
 * iterated many times. It will alternately compute X from Y, then Y from X. In the first case, for
 * iteration i, input is from {@code tmp/iterations/[i-1]/Y} and output goes to
 * {@code tmp/iterations/[i]/X}. In the second case, it's {@code tmp/iterations/[i]/X} going to
 * {code tmp/iterations/[i]/Y}. Input and output are rows of a matrix: varlong mapped to floatarray.</p>
 *
 * <p>The core ALS computation here is fairly straightforward, but, there is complexity around
 * side-loading the X or Y matrix, rather than literally joining the matrix on the input, which would
 * be far too slow. To conserve memory, only keys that are expected to appear in the input to the
 * particular reducer will be loaded.</p>
 *
 * <h3>PublishXStep / PublishYStep</h3>
 *
 * <p>Simple map-only jobs that publish the X or Y matrix, respectively, from the final iteration, in a
 * form that's consumable by the Serving Layer. The output is compressed text format. See the source
 * code for exact format. It is otherwise the same matrix as computed by the iterations. The output
 * goes to {@code X/} and {@code Y/} respectively.</p>
 *
 * <h2>Recommend Steps</h2>
 *
 * <h3>DistributeRecommendWorkStep</h3>
 *
 * <p>This first step effectively joins data from {@code X/} (user-feature matrix) and {@code knownItems/},
 * since both are needed to recommend for a user. It writes the combined values as varlong mapped to
 * {@code IDAndKnownItemsAndFloatArrayWritable}. The output goes to {@code tmp/distributeRecommend/}.
 * This is necessary as its own step because the shuffle/combiner will need to be used differently in
 * the next step.</p>
 *
 * <h3>RecommendStep</h3>
 *
 * <p>This computes recommendations given output above. The item-features matrix, Y, is also needed.
 * It is side-loaded cleverly as in {@code RowStep}. Each reducer makes N recommendations per user when
 * N recommendations are desired. They will be condensed into N overall in the next step. The output
 * goes to {@code tmp/partialRecommend} and is encoded as varlong (user ID) mapped to a series of idvalue
 * objects representing top recommended items.</p>
 *
 * <h3>CollectRecommendStep</h3>
 *
 * <p>This stage simply takes output above and outputs final N recommendations for each user. Output
 * is conceptually similar, but in text format, and goes to {@code recommend/}.
 *
 *
 * <h2>Similarity Steps</h2>
 *
 * <h3>DistributeSimilarWorkStep</h3>
 *
 * <p>Quite analogous to {@code DistributeRecommendWorkStep} and {@code RecommendStep} combined. Because
 * there is no join needed, these two tasks can be accomplished in a single job.</p>
 *
 * <h3>SimilarStep</h3>
 *
 * <p>Quite analogous to {@code CollectRecommendStep}.</p>

 * @author Sean Owen
 */
public final class ALSDistributedGenerationRunner extends DistributedGenerationRunner {

  private static final Logger log = LoggerFactory.getLogger(ALSDistributedGenerationRunner.class);

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPreDependencies() {
    List<DependsOn<Class<? extends JobStep>>> preDeps = Lists.newArrayList();
    preDeps.add(DependsOn.<Class<? extends JobStep>>first(MergeIDMappingStep.class));
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(ToUserVectorsStep.class, MergeNewOldStep.class));
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(ToItemVectorsStep.class, MergeNewOldStep.class));
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(CollectKnownItemsStep.class, ToUserVectorsStep.class));
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(InitialYStep.class, ToItemVectorsStep.class));
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(PopularUserStep.class, ToUserVectorsStep.class));
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(PopularItemStep.class, ToItemVectorsStep.class));
    return preDeps;
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getIterationDependencies() {
    List<DependsOn<Class<? extends JobStep>>> iterationsDeps = Lists.newArrayList();
    iterationsDeps.add(DependsOn.<Class<? extends JobStep>>first(RowStep.class));
    return iterationsDeps;
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPostDependencies() {
    Config config = ConfigUtils.getDefaultConfig();
    List<DependsOn<Class<? extends JobStep>>> postDeps = Lists.newArrayList();
    postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishXStep.class));
    postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishYStep.class));
    if (config.getBoolean("model.recommend.compute")) {
      postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(RecommendStep.class,
                                                                      DistributeRecommendWorkStep.class));
      postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(CollectRecommendStep.class, RecommendStep.class));
    }
    if (config.getBoolean("model.item-similarity.compute")) {
      postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(SimilarStep.class, DistributeSimilarWorkStep.class));
    }
    return postDeps;
  }

  @Override
  protected JobStepConfig buildConfig(int iteration) {
    return buildConfig(iteration, false);
  }

  private JobStepConfig buildConfig(int iteration, boolean isComputingX) {
    return new ALSJobStepConfig(getInstanceDir(),
                                getGenerationID(),
                                getLastGenerationID(),
                                iteration,
                                isComputingX);
  }

  @Override
  protected void runOneIteration(int iterationNumber, List<Collection<Class<? extends JobStep>>> iterationSchedule)
      throws InterruptedException, JobException, IOException {
    for (Collection<Class<? extends JobStep>> iterationStepClasses : iterationSchedule) {
      runSchedule(iterationStepClasses, buildConfig(iterationNumber, true)); // X
    }
    for (Collection<Class<? extends JobStep>> iterationStepClasses : iterationSchedule) {
      runSchedule(iterationStepClasses, buildConfig(iterationNumber, false)); // Y
    }
  }

  @Override
  protected boolean areIterationsDone(int iterationNumber) throws IOException {
    if (iterationNumber < 2) {
      return false;
    }

    Config config = ConfigUtils.getDefaultConfig();
    int maxIterations = config.getInt("model.iterations.max");

    if (maxIterations > 0 && iterationNumber >= maxIterations) {
      log.info("Reached iteration limit");
      return true;
    }

    String iterationsPrefix = Namespaces.getIterationsPrefix(getInstanceDir(), getGenerationID());

    LongObjectMap<LongFloatMap> previousEstimates =
        readUserItemEstimates(iterationsPrefix + (iterationNumber-1) + "/Yconvergence/");
    LongObjectMap<LongFloatMap> estimates =
        readUserItemEstimates(iterationsPrefix + iterationNumber + "/Yconvergence/");
    Preconditions.checkState(estimates.size() == previousEstimates.size(),
                             "Estimates and previous estimates not the same size: %s vs %s",
                             estimates.size(), previousEstimates.size());

    DoubleWeightedMean averageAbsoluteEstimateDiff = new DoubleWeightedMean();
    for (LongObjectMap.MapEntry<LongFloatMap> entry : estimates.entrySet()) {
      long userID = entry.getKey();
      LongFloatMap itemEstimates = entry.getValue();
      LongFloatMap previousItemEstimates = previousEstimates.get(userID);
      Preconditions.checkState(itemEstimates.size() == previousItemEstimates.size(),
                               "Number of estaimtes doesn't match previous: {} vs {}",
                               itemEstimates.size(), previousItemEstimates.size());
      for (LongFloatMap.MapEntry entry2 : itemEstimates.entrySet()) {
        long itemID = entry2.getKey();
        float estimate = entry2.getValue();
        float previousEstimate = previousItemEstimates.get(itemID);
        // Weight, simplistically, by newValue to emphasize effect of good recommendations.
        // But that only makes sense where newValue > 0
        if (estimate > 0.0f) {
          averageAbsoluteEstimateDiff.increment(FastMath.abs(estimate - previousEstimate), estimate);
        }
      }
    }

    double convergenceValue;
    if (averageAbsoluteEstimateDiff.getN() == 0) {
      // Fake value to cover corner case
      convergenceValue = FastMath.pow(2.0, -(iterationNumber + 1));
      log.info("No samples for convergence; using artificial convergence value: {}", convergenceValue);
    } else {
      convergenceValue = averageAbsoluteEstimateDiff.getResult();
      log.info("Avg absolute difference in estimate vs prior iteration: {}", convergenceValue);
      if (!Doubles.isFinite(convergenceValue)) {
        log.warn("Invalid convergence value, aborting iteration! {}", convergenceValue);
        return true;
      }
    }

    double convergenceThreshold = config.getDouble("model.iterations.convergence-threshold");
    if (convergenceValue < convergenceThreshold) {
      log.info("Converged");
      return true;
    }
    return false;
  }

  private static LongObjectMap<LongFloatMap> readUserItemEstimates(String convergenceSamplePrefix)
      throws IOException {
    log.info("Reading estimates from {}", convergenceSamplePrefix);
    LongObjectMap<LongFloatMap> userItemEstimate = new LongObjectMap<LongFloatMap>();
    Store store = Store.get();
    for (String prefix : store.list(convergenceSamplePrefix, true)) {
      for (CharSequence line : new FileLineIterable(store.readFrom(prefix))) {
        String[] tokens = DelimitedDataUtils.decode(line);
        long userID = Long.parseLong(tokens[0]);
        long itemID = Long.parseLong(tokens[1]);
        float estimate = Float.parseFloat(tokens[2]);
        LongFloatMap itemEstimate = userItemEstimate.get(userID);
        if (itemEstimate == null) {
          itemEstimate = new LongFloatMap();
          userItemEstimate.put(userID, itemEstimate);
        }
        itemEstimate.put(itemID, estimate);
      }
    }
    return userItemEstimate;
  }

  @Override
  protected void doPost() throws IOException {
    log.info("Loading X and Y to test whether they have sufficient rank");

    String generationPrefix = Namespaces.getInstanceGenerationPrefix(getInstanceDir(), getGenerationID());
    String xPrefix = generationPrefix + "X/";
    String yPrefix = generationPrefix + "Y/";

    Store store = Store.get();
    if (!doesXorYHasSufficientRank(xPrefix) || !doesXorYHasSufficientRank(yPrefix)) {
      log.warn("X or Y does not have sufficient rank; deleting this model and its results");
      store.recursiveDelete(xPrefix);
      store.recursiveDelete(yPrefix);
      store.recursiveDelete(generationPrefix + "recommend/");
      store.recursiveDelete(generationPrefix + "similarItems/");
    } else {
      log.info("X and Y have sufficient rank");
      File tempModelDescriptionFile = File.createTempFile("model-", ".pmml.gz");
      tempModelDescriptionFile.deleteOnExit();
      ALSModelDescription modelDescription = new ALSModelDescription();
      modelDescription.setKnownItemsPath("knownItems");
      modelDescription.setXPath("X");
      modelDescription.setYPath("Y");
      modelDescription.setIDMappingPath("idMapping");
      ALSModelDescription.write(tempModelDescriptionFile, modelDescription);
      store.upload(generationPrefix + "model.pmml.gz", tempModelDescriptionFile, false);
      IOUtils.deleteRecursively(tempModelDescriptionFile);
    }

    super.doPost();
  }

  private static boolean doesXorYHasSufficientRank(String xOrYPrefix) throws IOException {
    double[][] transposeTimesSelf = null;
    Store store = Store.get();
    for (String xOrYFilePrefix : store.list(xOrYPrefix, true)) {
      for (String line : new FileLineIterable(store.readFrom(xOrYFilePrefix))) {
        int tab = line.indexOf('\t');

        float[] elements = DataUtils.readFeatureVector(line.substring(tab + 1));
        int features = elements.length;

        if (transposeTimesSelf == null) {
          transposeTimesSelf = new double[features][features];
        }

        for (int i = 0; i < features; i++) {
          double rowFactor = elements[i];
          double[] transposeTimesSelfRow = transposeTimesSelf[i];
          for (int j = 0; j < features; j++) {
            transposeTimesSelfRow[j] += rowFactor * elements[j];
          }
        }

      }
      
      // Hacky but efficient and principled: if a portion of the matrix times itself is non-singular,
      // it's all but impossible that it would be singular when the rest was loaded
      
      if (transposeTimesSelf != null && 
          transposeTimesSelf.length != 0 && 
          MatrixUtils.isNonSingular(new Array2DRowRealMatrix(transposeTimesSelf))) {
        return true;
      } else {
        log.info("Matrix is not yet proved to be non-singular, continuing to load...");
      }
    }
    return false;
  }

}
