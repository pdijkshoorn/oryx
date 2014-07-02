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

import com.cloudera.oryx.als.common.pmml.ALSModelDescription;
import com.cloudera.oryx.als.computation.ALSJobStepConfig;
import com.cloudera.oryx.als.computation.modelbuilder.ALSModelBuilder;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.OryxConfiguration;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public class ALSSparkGenerationRunner extends LocalGenerationRunner {

  public static final String CONFIG_SERIALIZATION_KEY = "CONFIG_SERIALIZATION";

  private static final Logger log = LoggerFactory.getLogger(ALSSparkGenerationRunner.class);

  private final String sparkMaster;

  public ALSSparkGenerationRunner() { this(ConfigUtils.getDefaultConfig().getString("model.spark-master")); }

  public ALSSparkGenerationRunner(String sparkMaster) {
    this.sparkMaster = Preconditions.checkNotNull(sparkMaster);
  }

  @Override
  protected void runSteps() throws IOException, JobException, InterruptedException {
    String instanceDir = getInstanceDir();
    int generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);

    Store store = Store.get();

    ALSModelBuilder modelBuilder = new ALSModelBuilder(store);
    ALSJobStepConfig jobStepConfig = new ALSJobStepConfig(getInstanceDir(), getGenerationID(), getLastGenerationID(),
        0, false);
    Pipeline sp = new SparkPipeline(sparkMaster, "ALS", this.getClass());

    Configuration conf = sp.getConfiguration();
    copyConf(OryxConfiguration.get(), conf);
    conf.set(CONFIG_SERIALIZATION_KEY, ConfigUtils.getDefaultConfig().root().render());

    modelBuilder.build(sp.readTextFile(generationPrefix + "inbound/"), jobStepConfig);

    File tempOutDir = Files.createTempDir();
    File tempModelDescriptionFile = new File(tempOutDir, "model.pmml.gz");
    ALSModelDescription modelDescription = new ALSModelDescription();
    modelDescription.setKnownItemsPath("knownItems");
    modelDescription.setXPath("X");
    modelDescription.setYPath("Y");
    modelDescription.setIDMappingPath("idMapping");
    ALSModelDescription.write(tempModelDescriptionFile, modelDescription);

    store.uploadDirectory(generationPrefix, tempOutDir, false);
    sp.done();
  }

  static void copyConf(Configuration from, Configuration to) {
    ByteArrayDataOutput dado = ByteStreams.newDataOutput();
    try {
      from.write(dado);
      to.readFields(ByteStreams.newDataInput(dado.toByteArray()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
