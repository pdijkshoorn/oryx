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

package com.cloudera.oryx.als.computation.local;

import com.google.common.io.Files;
import com.typesafe.config.Config;

import java.io.File;
import java.io.IOException;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.factorizer.MatrixFactorizer;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;

public final class ALSLocalGenerationRunner extends LocalGenerationRunner {

	
  @Override
  protected void runSteps() throws IOException, InterruptedException, JobException {

    String instanceDir = getInstanceDir();
    int generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    int lastGenerationID = getLastGenerationID();

    File currentInboundDir = Files.createTempDir();
    currentInboundDir.deleteOnExit();
    File currentTrainDir = Files.createTempDir();
    currentTrainDir.deleteOnExit();
    File tempOutDir = Files.createTempDir();
    tempOutDir.deleteOnExit();
    File currentTestDir = new File(tempOutDir, "test");

    File lastInputDir = null;
    File lastMappingDir = null;
    File lastTestDir = null;
    if (lastGenerationID >= 0) {
      lastInputDir = Files.createTempDir();
      lastInputDir.deleteOnExit();
      lastMappingDir = Files.createTempDir();
      lastMappingDir.deleteOnExit();
      lastTestDir = Files.createTempDir();
      lastTestDir.deleteOnExit();
    }

    try {

      Store store = Store.get();
      store.downloadDirectory(generationPrefix + "inbound/", currentInboundDir);
      if (lastGenerationID >= 0) {
        String lastGenerationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID);
        store.downloadDirectory(lastGenerationPrefix + "input/", lastInputDir);
        store.downloadDirectory(lastGenerationPrefix + "idMapping/", lastMappingDir);
        store.downloadDirectory(lastGenerationPrefix + "test/", lastTestDir);
      }

      new SplitTestTrain(currentInboundDir, currentTrainDir, currentTestDir).call();

      Config config = ConfigUtils.getDefaultConfig();

      boolean noKnownItems = config.getBoolean("model.no-known-items");
      LongObjectMap<LongSet> knownItemIDs = noKnownItems ? null : new LongObjectMap<LongSet>();
      LongObjectMap<LongFloatMap> RbyRow = new LongObjectMap<LongFloatMap>();
      LongObjectMap<LongFloatMap> RbyColumn = new LongObjectMap<LongFloatMap>();
      StringLongMapping idMapping = new StringLongMapping();

      if (lastGenerationID >= 0) {
        new ReadInputs(lastInputDir, false, knownItemIDs, RbyRow, RbyColumn, idMapping).call();
        new ReadInputs(lastTestDir, false, knownItemIDs, RbyRow, RbyColumn, idMapping).call();
        new ReadMapping(lastMappingDir, idMapping).call();
      }
      new ReadInputs(currentTrainDir, true, knownItemIDs, RbyRow, RbyColumn, idMapping).call();

      if (RbyRow.isEmpty() || RbyColumn.isEmpty()) {
        return;
      }

      MatrixFactorizer als = new FactorMatrix(RbyRow, RbyColumn).call();

      new WriteOutputs(tempOutDir, RbyRow, knownItemIDs, als.getX(), als.getY(), idMapping).call();

      if (config.getDouble("model.test-set-fraction") > 0.0) {
        new ComputeMAP(currentTestDir, als.getX(), als.getY()).call();
      }

      if (config.getBoolean("model.recommend.compute")) {
        new MakeRecommendations(tempOutDir, knownItemIDs, als.getX(), als.getY(), idMapping).call();

      }

      if (config.getBoolean("model.item-similarity.compute")) {
        new MakeItemSimilarity(tempOutDir, als.getY(), idMapping).call();
      }

      store.uploadDirectory(generationPrefix, tempOutDir, false);

    } finally {
      IOUtils.deleteRecursively(currentInboundDir);
      IOUtils.deleteRecursively(currentTrainDir);
      IOUtils.deleteRecursively(currentTestDir);
      IOUtils.deleteRecursively(tempOutDir);
      IOUtils.deleteRecursively(lastInputDir);
      IOUtils.deleteRecursively(lastTestDir);
      Config myConfig = ConfigUtils.getDefaultConfig();
      if (myConfig.getBoolean("model.recommend.specificUsers"))
      	System.exit(0);
    }
  }

}
