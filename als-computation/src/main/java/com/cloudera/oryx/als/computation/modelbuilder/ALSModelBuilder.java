/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.oryx.als.computation.modelbuilder;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.computation.ALSJobStepConfig;
import com.cloudera.oryx.als.computation.initialy.CopyPreviousYFn;
import com.cloudera.oryx.als.computation.initialy.FlagNewItemsFn;
import com.cloudera.oryx.als.computation.initialy.InitialYReduceFn;
import com.cloudera.oryx.als.computation.initialy.PreviousOrEmptyFeaturesAggregator;
import com.cloudera.oryx.als.computation.iterate.row.ConvergenceSampleFn;
import com.cloudera.oryx.als.computation.iterate.row.RowReduceFn;
import com.cloudera.oryx.als.computation.iterate.row.YState;
import com.cloudera.oryx.als.computation.known.CollectKnownItemsFn;
import com.cloudera.oryx.als.computation.merge.CombineMappingsFn;
import com.cloudera.oryx.als.computation.merge.DelimitedInputParseFn;
import com.cloudera.oryx.als.computation.merge.ExistingMappingsMapFn;
import com.cloudera.oryx.als.computation.merge.InboundJoinFn;
import com.cloudera.oryx.als.computation.merge.JoinBeforeMapFn;
import com.cloudera.oryx.als.computation.merge.MappingParseFn;
import com.cloudera.oryx.als.computation.merge.MergeNewOldValuesFn;
import com.cloudera.oryx.als.computation.merge.SplitTestFn;
import com.cloudera.oryx.als.computation.merge.ToVectorReduceFn;
import com.cloudera.oryx.als.computation.merge.TransposeUserItemFn;
import com.cloudera.oryx.als.computation.popular.PopularMapFn;
import com.cloudera.oryx.als.computation.popular.PopularReduceFn;
import com.cloudera.oryx.als.computation.publish.PublishMapFn;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.random.RandomUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.stats.DoubleWeightedMean;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.modelbuilder.AbstractModelBuilder;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.typesafe.config.Config;
import org.apache.commons.math3.util.FastMath;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.crunch.lib.Channels;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

public class ALSModelBuilder extends AbstractModelBuilder<String, String, ALSJobStepConfig> {

  private static final Logger log = LoggerFactory.getLogger(ALSModelBuilder.class);

  private static final PTableType<Pair<Long, Integer>, NumericIDValue> JOIN_TYPE = Avros.tableOf(
      Avros.pairs(ALSTypes.LONGS, ALSTypes.INTS),
      ALSTypes.IDVALUE);

  private final Store store;

  public ALSModelBuilder() {
    this(Store.get());
  }

  public ALSModelBuilder(Store store) {
    this.store = Preconditions.checkNotNull(store);
  }

  @Override
  public String build(PCollection<String> inbound, ALSJobStepConfig jobStepConfig) throws IOException {

    String instanceDir = jobStepConfig.getInstanceDir();
    int generationID = jobStepConfig.getGenerationID();
    String instancePrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String tempPrefix = Namespaces.getTempPrefix(instanceDir, generationID);
    String iterationPrefix = Namespaces.getIterationsPrefix(instanceDir, generationID);

    Pipeline p = inbound.getPipeline();
    Configuration conf = p.getConfiguration();

    mergeIdMapping(inbound, getPreviousIdMapping(p, jobStepConfig))
        .write(compressedTextOutput(conf, instancePrefix + "idMapping/"), Target.WriteMode.CHECKPOINT);

    Pair<PCollection<String>, PCollection<String>> trainTest = splitTrainTest(inbound);
    trainTest.first().write(compressedTextOutput(conf, instancePrefix + "train/"), Target.WriteMode.CHECKPOINT);
    trainTest.second().write(compressedTextOutput(conf, instancePrefix + "test/"), Target.WriteMode.CHECKPOINT);

    PTable<Long, NumericIDValue> input = mergeNewOld(trainTest.first(), getOld(p, jobStepConfig))
        .write(output(instancePrefix + "input/"), Target.WriteMode.CHECKPOINT);

    PTable<Long, LongFloatMap> userVectors = toVectors(input)
        .write(output(tempPrefix + "userVectors/"), Target.WriteMode.CHECKPOINT);
    popular(userVectors)
        .write(output(tempPrefix + "popularItemsByUserPartition/"), Target.WriteMode.CHECKPOINT);
    collectKnownItems(userVectors)
        .write(compressedTextOutput(conf, instancePrefix + "knownItems/"), Target.WriteMode.CHECKPOINT);

    PTable<Long, LongFloatMap> itemVectors = toVectors(transpose(input))
        .write(output(tempPrefix + "itemVectors/"), Target.WriteMode.CHECKPOINT);

    popular(itemVectors)
        .write(output(tempPrefix + "popularUsersByItemPartition/"), Target.WriteMode.CHECKPOINT);

    PCollection<MatrixRow> initialY = initialY(itemVectors, getLastInitialY(p, jobStepConfig))
        .write(output(iterationPrefix + "0/Y/"), Target.WriteMode.CHECKPOINT);

    p.run();

    boolean iterationsDone = store.exists(instancePrefix + "X/", false);
    int iteration = 1;
    PCollection<MatrixRow> X = null, Y = initialY;
    Iterable<String> prevSample = null;
    while (!iterationsDone) {
      String iterationKey = iterationPrefix + iteration + "/";

      // X Phase
      X = update(userVectors, new YState(ALSTypes.DENSE_ROW_MATRIX, tempPrefix + "popularItemsByUserPartition/",
          iterationPrefix + (iteration - 1) + "/Y/"))
          .write(output(iterationKey + "X/"), Target.WriteMode.CHECKPOINT);
      p.run();

      // Y Phase
      YState yState = new YState(ALSTypes.DENSE_ROW_MATRIX,
          tempPrefix + "popularUsersByItemPartition/",
          iterationKey + "X/");
      Y = update(itemVectors, yState).write(output(iterationKey + "Y/"), Target.WriteMode.CHECKPOINT);
      // Convergence sampling
      Iterable<String> sample = convergenceSample(Y, yState).materialize();
      p.run();

      iterationsDone = areIterationsDone(iteration, jobStepConfig, sample, prevSample);
      iteration++;
      prevSample = sample;
    }

    if (X != null && Y != null) {
      publish(X).write(compressedTextOutput(conf, instancePrefix + "X/"), Target.WriteMode.CHECKPOINT);
      publish(Y).write(compressedTextOutput(conf, instancePrefix + "Y/"), Target.WriteMode.CHECKPOINT);
      p.run();
    } else {
      log.info("X/ and Y/ already exist under {}, nothing to do", instancePrefix);
    }

    return instancePrefix;
  }


  PCollection<String> convergenceSample(PCollection<MatrixRow> matrix, YState yState) {
    int modulus = chooseConvergenceSamplingModulus(getNumReducers());
    return matrix
        .parallelDo("asPair", MatrixRow.AS_PAIR, Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY))
        .parallelDo("convergenceSample", new ConvergenceSampleFn(yState, modulus), Avros.strings());
  }

  private static int chooseConvergenceSamplingModulus(int numReducers) {
    // Kind of arbitrary formula, determined empirically.
    int modulus = RandomUtils.nextTwinPrime(16 * numReducers * numReducers);
    log.info("Using convergence sampling modulus {} to sample about {}% of all user-item pairs for convergence",
        modulus, 100.0f / modulus / modulus);
    return modulus;
  }

  protected boolean areIterationsDone(int iterationNumber, JobStepConfig jobStepConfig,
                                      Iterable<String> sample,
                                      Iterable<String> lastSample) throws IOException {

    // First maybe output MAP
    String iterationsPrefix = Namespaces.getIterationsPrefix(jobStepConfig.getInstanceDir(),
        jobStepConfig.getGenerationID());

    String mapKey = iterationsPrefix + iterationNumber + "/MAP";
    if (store.exists(mapKey, true)) {
      double map;
      BufferedReader in = store.readFrom(mapKey);
      try {
        map = Double.parseDouble(in.readLine());
      } finally {
        in.close();
      }
      log.info("Mean average precision estimate: {}", map);
    }

    if (iterationNumber < 2) {
      return false;
    }

    Config config = getConfig();
    int maxIterations = config.getInt("model.iterations.max");

    if (maxIterations > 0 && iterationNumber >= maxIterations) {
      log.info("Reached iteration limit");
      return true;
    }

    if (lastSample == null) {
      // Previous iteration was deleted already because next iteration failed to start; continue
      return false;
    }

    LongObjectMap<LongFloatMap> previousEstimates = readUserItemEstimates(lastSample);
    LongObjectMap<LongFloatMap> estimates = readUserItemEstimates(sample);
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

  private static LongObjectMap<LongFloatMap> readUserItemEstimates(Iterable<String> sample)
      throws IOException {
    log.info("Reading estimates from {}", sample);
    LongObjectMap<LongFloatMap> userItemEstimate = new LongObjectMap<LongFloatMap>();
    for (CharSequence line : sample) {
      String[] tokens = DelimitedDataUtils.decode(line, ',');
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
    return userItemEstimate;
  }

  PCollection<String> publish(PCollection<MatrixRow> matrix) {
    return matrix.parallelDo("publish", new PublishMapFn(), Avros.strings());
  }

  PCollection<MatrixRow> update(PTable<Long, LongFloatMap> vectors, YState yState) {
    return vectors.groupByKey(groupingOptions())
        .parallelDo("rowReduce", new RowReduceFn(yState), ALSTypes.DENSE_ROW_MATRIX);
  }

  PCollection<MatrixRow> initialY(PTable<Long, LongFloatMap> itemVectors, PTable<Long, float[]> last) {
    return itemVectors
        .parallelDo("flagNewItems", new FlagNewItemsFn(), Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY))
        .union(last)
        .groupByKey(groupingOptions())
        .combineValues(new PreviousOrEmptyFeaturesAggregator())
        .parallelDo("initialYReduce", new InitialYReduceFn(), ALSTypes.DENSE_ROW_MATRIX);
  }

  PTable<Long, float[]> getLastInitialY(Pipeline p, ALSJobStepConfig jobConfig) throws IOException {
    int lastGenerationID = jobConfig.getLastGenerationID();
    if (lastGenerationID >= 0) {
      String instanceDir = jobConfig.getInstanceDir();
      String yPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "Y/";
      if (store.exists(yPrefix, false)) {
        return PTables.asPTable(p.read(textInput(yPrefix))
            .parallelDo("copyOld", new CopyPreviousYFn(), Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY)));
      } else {
        log.warn("Previous generation exists, but no Y; this should only happen if the model was not generated " +
            "due to insufficient rank");
      }
    }
    return p.emptyPTable(Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY));
  }

  PCollection<Long> popular(PCollection<Pair<Long, LongFloatMap>> vectors) {
    return vectors
        .parallelDo("popularMap", new PopularMapFn(), Avros.tableOf(ALSTypes.INTS, ALSTypes.ID_SET))
        .groupByKey(groupingOptions())
            //.combineValues(new FastIDSetAggregator())
        .parallelDo("popularReduce", new PopularReduceFn(), ALSTypes.LONGS);
  }

  PCollection<String> collectKnownItems(PCollection<Pair<Long, LongFloatMap>> input) {
    return input.parallelDo("collectKnownItems", new CollectKnownItemsFn(), Avros.strings());
  }

  PTable<Long, NumericIDValue> transpose(PTable<Long, NumericIDValue> input) {
    return input.parallelDo("transposeUserItem", new TransposeUserItemFn(),
        Avros.tableOf(ALSTypes.LONGS, ALSTypes.IDVALUE));
  }

  PTable<Long, LongFloatMap> toVectors(PTable<Long, NumericIDValue> input) {
    return PTables.asPTable(input
        .groupByKey(groupingOptions())
        .parallelDo("toVectors", new ToVectorReduceFn(), ALSTypes.SPARSE_ROW_MATRIX));
  }

  PTable<Pair<Long, Integer>, NumericIDValue> getOld(Pipeline p, ALSJobStepConfig jobConfig) throws IOException {
    int lastGenerationID = jobConfig.getLastGenerationID();
    if (lastGenerationID >= 0) {
      String instanceDir = jobConfig.getInstanceDir();
      String inputPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "input/";
      Preconditions.checkState(store.exists(inputPrefix, false), "Input path does not exist: %s", inputPrefix);
      PTable<Pair<Long, Integer>, NumericIDValue> ret = p.read(input(inputPrefix, ALSTypes.VALUE_MATRIX))
          .parallelDo("lastGenerationInput", new JoinBeforeMapFn(), JOIN_TYPE);

      String testPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "test/";
      if (store.exists(testPrefix, false)) {
        // Held out test data last time; incorporate now
        PTable<Pair<Long, Integer>, NumericIDValue> joinTestSetBefore = p.read(textInput(testPrefix))
            .parallelDo("testParse", new DelimitedInputParseFn(), Avros.pairs(ALSTypes.LONGS, ALSTypes.IDVALUE))
            .parallelDo("lastGenerationTest", new JoinBeforeMapFn(), JOIN_TYPE);
        ret = ret.union(joinTestSetBefore);
      }
      return ret;
    } else {
      return p.emptyPTable(JOIN_TYPE);
    }
  }

  PTable<Long, NumericIDValue> mergeNewOld(
      PCollection<String> rawTrain,
      PTable<Pair<Long, Integer>, NumericIDValue> previous) {

    return PTables.asPTable(rawTrain
        .parallelDo("trainParse", new DelimitedInputParseFn(), Avros.pairs(ALSTypes.LONGS, ALSTypes.IDVALUE))
        .parallelDo("train", new InboundJoinFn(), JOIN_TYPE)
        .union(previous)
        .groupByKey(
            GroupingOptions.builder()
                .partitionerClass(JoinUtils.getPartitionerClass(AvroTypeFamily.getInstance()))
                .numReducers(getNumReducers())
            .build())
        .parallelDo("mergeNewOld", new MergeNewOldValuesFn(), ALSTypes.VALUE_MATRIX));
  }

  PTable<Long, String> getPreviousIdMapping(Pipeline p, ALSJobStepConfig jobConfig) throws IOException {
    int lastGenerationID = jobConfig.getLastGenerationID();
    if (lastGenerationID >= 0) {
      String instanceDir = jobConfig.getInstanceDir();
      String idMappingPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "idMapping/";
      Preconditions.checkState(store.exists(idMappingPrefix, false), "Input path does not exist: %s", idMappingPrefix);
      return p.read(textInput(idMappingPrefix)).parallelDo("lastGeneration", new ExistingMappingsMapFn(),
          Avros.tableOf(ALSTypes.LONGS, Avros.strings()));
    } else {
      return p.emptyPTable(Avros.tableOf(ALSTypes.LONGS, Avros.strings()));
    }
  }

  PCollection<String> mergeIdMapping(PCollection<String> inbound, PTable<Long, String> previous) {
    return inbound
        .parallelDo("inboundParseForMapping", new MappingParseFn(), Avros.tableOf(ALSTypes.LONGS, Avros.strings()))
        .union(previous)
        .groupByKey(groupingOptions()) // TODO
        .parallelDo("mergeNewOldMappings", new CombineMappingsFn(), Avros.strings());
  }

  Pair<PCollection<String>, PCollection<String>> splitTrainTest(PCollection<String> inbound) {
    double testSetFraction = getConfig().getDouble("model.test-set-fraction");
    return Channels.split(
        inbound.parallelDo("splitTrainTest", new SplitTestFn(testSetFraction),
            Avros.pairs(Avros.strings(), Avros.strings()))
    );
  }
}
