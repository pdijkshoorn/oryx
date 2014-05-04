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
package com.cloudera.oryx.als.computation.local;

import com.cloudera.oryx.als.common.DataUtils;
import com.cloudera.oryx.als.common.pmml.ALSModelDescription;
import com.cloudera.oryx.als.computation.ALSJobStepConfig;
import com.cloudera.oryx.als.computation.modelbuilder.ALSModelBuilder;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.math.MatrixUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import com.google.common.io.Files;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ALSLocalSparkGenerationRunner extends LocalGenerationRunner {

  private static final Logger log = LoggerFactory.getLogger(ALSLocalSparkGenerationRunner.class);

  @Override
  protected void runSteps() throws IOException, JobException, InterruptedException {
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

      ALSModelBuilder modelBuilder = new ALSModelBuilder(store);
      Pipeline sp = new SparkPipeline("local[4]", "als", this.getClass());
      modelBuilder.build(sp.readTextFile(currentInboundDir.getAbsolutePath()),
          new ALSJobStepConfig(getInstanceDir(), getGenerationID(), getLastGenerationID(), 0, false));

      File tempModelDescriptionFile = new File(tempOutDir, "model.pmml.gz");
      ALSModelDescription modelDescription = new ALSModelDescription();
      modelDescription.setKnownItemsPath("knownItems");
      modelDescription.setXPath("X");
      modelDescription.setYPath("Y");
      modelDescription.setIDMappingPath("idMapping");
      ALSModelDescription.write(tempModelDescriptionFile, modelDescription);

      store.uploadDirectory(generationPrefix, tempOutDir, false);
      sp.done();

    } finally {
      IOUtils.deleteRecursively(currentInboundDir);
      IOUtils.deleteRecursively(currentTrainDir);
      IOUtils.deleteRecursively(currentTestDir);
      IOUtils.deleteRecursively(tempOutDir);
      IOUtils.deleteRecursively(lastInputDir);
      IOUtils.deleteRecursively(lastTestDir);
    }
  }
}
