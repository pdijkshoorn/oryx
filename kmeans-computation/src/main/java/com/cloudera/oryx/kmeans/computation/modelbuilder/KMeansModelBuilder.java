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
package com.cloudera.oryx.kmeans.computation.modelbuilder;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.crossfold.Crossfold;
import com.cloudera.oryx.computation.common.fn.StringSplitFn;
import com.cloudera.oryx.computation.common.modelbuilder.AbstractModelBuilder;
import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.RecordSpec;
import com.cloudera.oryx.computation.common.records.Spec;
import com.cloudera.oryx.computation.common.sample.ReservoirSampling;
import com.cloudera.oryx.computation.common.summary.Summarizer;
import com.cloudera.oryx.computation.common.types.Serializables;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.KMeansJobStepConfig;
import com.cloudera.oryx.kmeans.computation.MLAvros;
import com.cloudera.oryx.kmeans.computation.cluster.CentersIndexLoader;
import com.cloudera.oryx.kmeans.computation.cluster.ClusterSettings;
import com.cloudera.oryx.kmeans.computation.cluster.DistanceToClosestFn;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import com.cloudera.oryx.kmeans.computation.cluster.UpdateIndexFn;
import com.cloudera.oryx.kmeans.computation.evaluate.CentersOutputFn;
import com.cloudera.oryx.kmeans.computation.evaluate.ClosestSketchVectorAggregator;
import com.cloudera.oryx.kmeans.computation.evaluate.ClosestSketchVectorData;
import com.cloudera.oryx.kmeans.computation.evaluate.ClosestSketchVectorFn;
import com.cloudera.oryx.kmeans.computation.evaluate.EvaluationSettings;
import com.cloudera.oryx.kmeans.computation.evaluate.KMeansClusteringFn;
import com.cloudera.oryx.kmeans.computation.evaluate.KMeansEvaluationData;
import com.cloudera.oryx.kmeans.computation.evaluate.ReplicateValuesFn;
import com.cloudera.oryx.kmeans.computation.evaluate.StatsOutputFn;
import com.cloudera.oryx.kmeans.computation.evaluate.WeightVectorsFn;
import com.cloudera.oryx.kmeans.computation.normalize.NormalizeSettings;
import com.cloudera.oryx.kmeans.computation.normalize.StandardizeFn;
import com.cloudera.oryx.kmeans.computation.types.KMeansTypes;
import com.typesafe.config.Config;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class KMeansModelBuilder extends AbstractModelBuilder<Record, String, KMeansJobStepConfig> {

  private static final Logger log = LoggerFactory.getLogger(KMeansModelBuilder.class);

  @Override
  public String build(PCollection<Record> records, KMeansJobStepConfig jobStepConfig) throws IOException {
    String instanceDir = jobStepConfig.getInstanceDir();
    int generationID = jobStepConfig.getGenerationID();
    String instancePrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);

    Pipeline p = records.getPipeline();
    Configuration conf = p.getConfiguration();
    Config defaultConfig = ConfigUtils.getDefaultConfig();
    boolean doOutliers = doOutlierComputation(defaultConfig);
    InboundSettings inboundSettings = InboundSettings.create(defaultConfig);
    if (doOutliers) {
      if (inboundSettings.getIdColumns().isEmpty()) {
        String msg = "id-column(s) value must be specified if model.outliers.compute is enabled";
        log.error(msg);
        throw new IllegalStateException("Invalid k-means configuration: " + msg);
      }
    }

    // Summarize the data
    String summaryKey = instancePrefix + "summary/";
    getSummarizer(inboundSettings).buildJson(records)
        .write(compressedTextOutput(conf, summaryKey), Target.WriteMode.CHECKPOINT);
    p.run();

    // Normalize the data into vectors using the summary, and apply the crossfold strategy
    NormalizeSettings normalizeSettings = NormalizeSettings.create(defaultConfig);
    String normalizeKey = instancePrefix + "normalized/";
    PCollection<Pair<Integer, RealVector>> folds = normalize(records,
        getStandardizeFn(inboundSettings, normalizeSettings, summaryKey),
        new Crossfold(defaultConfig.getInt("model.cross-folds")));
    folds.write(avroOutput(normalizeKey), Target.WriteMode.CHECKPOINT);
    p.run();

    // Iterate and build the sketches over the folds
    ClusterSettings clusterSettings = ClusterSettings.create(defaultConfig);
    int sketchIterations = defaultConfig.getInt("model.sketch-iterations");
    for (int iteration = 0; iteration < sketchIterations; iteration++) {
      DistanceToClosestFn<RealVector> distanceToClosestFn;
      UpdateIndexFn updateIndexFn;
      if (iteration == 0) {
        KSketchIndex index = createInitialIndex(clusterSettings, folds);
        distanceToClosestFn = new DistanceToClosestFn<RealVector>(index);
        updateIndexFn = new UpdateIndexFn(index);
      } else {
        // Get the index location from the previous iteration
        String previousIndexKey = instancePrefix + String.format("sketch/%d/", iteration);
        distanceToClosestFn = new DistanceToClosestFn<RealVector>(previousIndexKey);
        updateIndexFn = new UpdateIndexFn(previousIndexKey);
      }

      sketch(folds, distanceToClosestFn, updateIndexFn, clusterSettings)
          .write(avroOutput(instancePrefix + String.format("/sketch/%d/", iteration + 1)), Target.WriteMode.CHECKPOINT);
      p.run();
    }

    // Voronoi partition based on the sketches.
    String lastSketch = instancePrefix + String.format("/sketch/%d", sketchIterations);
    String voronoiPrefix = instancePrefix + "weighted/";
    PCollection<Pair<Integer, WeightedRealVector>> weightedPoints = weightPoints(folds, lastSketch, clusterSettings)
        .write(avroOutput(voronoiPrefix + "kSketchVectorWeights/"), Target.WriteMode.CHECKPOINT)
        .parallelDo(
            "generatingWeightedSketchVectors",
            new WeightVectorsFn(lastSketch),
            KMeansTypes.FOLD_WEIGHTED_VECTOR)
        .write(avroOutput(voronoiPrefix + "weightedKSketchVectors/"), Target.WriteMode.CHECKPOINT);

    EvaluationSettings evaluationSettings = EvaluationSettings.create(defaultConfig);
    String evalPrefix = instancePrefix + "eval/";
    PCollection<KMeansEvaluationData> evalData = evaluate(weightedPoints, evaluationSettings);
    evalData.parallelDo("replicaCenters", new CentersOutputFn(instancePrefix), Avros.strings())
        .write(compressedTextOutput(conf, evalPrefix + "replicaCenters/"));
    // Write out the per-replica stats
    evalData.parallelDo("replicaStats", new StatsOutputFn(), Avros.strings())
        .write(compressedTextOutput(conf, evalPrefix + "replicaStats/"));

    // Finish processing
    p.run();

    return instancePrefix;
  }

  PCollection<KMeansEvaluationData> evaluate(PCollection<Pair<Integer, WeightedRealVector>> weightedPoints,
                                             EvaluationSettings settings) {
     return weightedPoints
        .parallelDo("replicate",
            new ReplicateValuesFn<Pair<Integer, WeightedRealVector>>(settings.getKValues(), settings.getReplications()),
            Avros.tableOf(Avros.pairs(Avros.ints(), Avros.ints()), Avros.pairs(Avros.ints(), MLAvros.weightedVector())))
        .groupByKey(settings.getParallelism())
        .parallelDo("cluster",
            new KMeansClusteringFn(settings),
            Serializables.avro(KMeansEvaluationData.class));
  }

  PCollection<ClosestSketchVectorData> weightPoints(
      PCollection<Pair<Integer, RealVector>> folds,
      String indexKey,
      ClusterSettings clusterSettings) {
    return PTables.asPTable(folds)
        .parallelDo("computingSketchVectorWeights",
            new ClosestSketchVectorFn<RealVector>(indexKey, clusterSettings),
            Avros.tableOf(Avros.ints(), Avros.reflects(ClosestSketchVectorData.class)))
        .groupByKey(1)
        .combineValues(new ClosestSketchVectorAggregator(clusterSettings))
        .values();
  }

  PCollection<KSketchIndex> sketch(
      PCollection<Pair<Integer, RealVector>> folds,
      DistanceToClosestFn<RealVector> distanceToClosestFn,
      UpdateIndexFn updateIndexFn,
      ClusterSettings settings) {
    PTable<Integer, Pair<RealVector, Double>> weighted = folds.parallelDo("computeDistances", distanceToClosestFn,
        Avros.tableOf(Avros.ints(), Avros.pairs(MLAvros.vector(), Avros.doubles())));

    // run weighted reservoir sampling on the vector to select another group of settings.getSketchPoints()
    // to add to the k-sketch
    PTable<Integer,RealVector> kSketchSample = ReservoirSampling.groupedWeightedSample(weighted,
        settings.getSketchPoints(), RandomManager.getRandom());

    // update the KSketchIndex with the newly-chosen vectors
    return kSketchSample.parallelDo("updateIndex", updateIndexFn, Serializables.avro(KSketchIndex.class));
  }

  private PCollection<Pair<Integer, RealVector>> normalize(
      PCollection<Record> records,
      StandardizeFn standardizeFn,
      Crossfold cf) {
    return cf.apply(records.parallelDo("normalize", standardizeFn, MLAvros.vector()));
  }

  static StandardizeFn getStandardizeFn(InboundSettings inbound, NormalizeSettings settings, String summaryKey) {
    return new StandardizeFn(settings, inbound.getIgnoredColumns(), inbound.getIdColumns(), summaryKey);
  }

  static Summarizer getSummarizer(InboundSettings settings) {
    return new Summarizer()
        .spec(getSpec(settings))
        .categoricalColumns(settings.getCategoricalColumns())
        .ignoreColumns(settings.getIgnoredColumns())
        .ignoreColumns(settings.getIdColumns());
  }

  static Spec getSpec(InboundSettings settings) {
    List<String> columnNames = settings.getColumnNames();
    if (columnNames.isEmpty()) {
      return null;
    }
    RecordSpec.Builder rsb = RecordSpec.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      if (settings.isNumeric(i)) {
        rsb.add(columnName, DataType.DOUBLE);
      } else {
        rsb.add(columnName, DataType.STRING);
      }
    }
    return rsb.build();
  }

  <V extends RealVector> PCollection<Pair<Integer, V>> inputPairs(
      Pipeline p,
      String inputKey,
      PType<V> ptype) {
    PType<Pair<Integer, V>> inputType = Avros.pairs(Avros.ints(), ptype);
    return p.read(avroInput(inputKey, inputType));
  }

  <V extends RealVector> PCollection<V> inputVectors(Pipeline p, String inputKey, PType<V> ptype) {
    return PTables.asPTable(inputPairs(p, inputKey, ptype)).values();
  }

  static KSketchIndex createInitialIndex(
      ClusterSettings settings,
      PCollection<Pair<Integer, RealVector>> input) {
    RealVector[] init = new RealVector[settings.getCrossFolds()];
    for (Pair<Integer, RealVector> rv : input.materialize()) {
      if (init[rv.first()] == null) {
        init[rv.first()] = rv.second();
      }
      boolean done = true;
      for (RealVector vec : init) {
        if (vec == null) {
          done = false;
          break;
        }
      }
      if (done) {
        break;
      }
    }

    KSketchIndex index = new KSketchIndex(
        settings.getCrossFolds(),
        init[0].getDimension(),
        settings.getIndexBits(),
        settings.getIndexSamples(),
        1729L); // TODO: something smarter, or figure out that I don't need this b/c I compute the projections up front
    for (int i = 0; i < init.length; i++) {
      index.add(init[i], i);
    }
    return index;
  }

  KSketchIndex getCentersIndex(String prefix) throws IOException {
    return (new CentersIndexLoader(ClusterSettings.create(ConfigUtils.getDefaultConfig()))).load(prefix);
  }

  static boolean doOutlierComputation(Config config) {
    return config.hasPath("model.outliers") && config.getBoolean("model.outliers.compute");
  }

}
