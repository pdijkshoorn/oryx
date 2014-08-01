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
package com.cloudera.oryx.rdf.computation;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.OryxConfiguration;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import com.cloudera.oryx.rdf.computation.modelbuilder.RDFModelBuilder;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RDFSparkGenerationRunner extends LocalGenerationRunner {

  public static final String CONFIG_SERIALIZATION_KEY = "CONFIG_SERIALIZATION";

  private static final Logger log = LoggerFactory.getLogger(RDFSparkGenerationRunner.class);

  private final String sparkMaster;

  public RDFSparkGenerationRunner() { this(ConfigUtils.getDefaultConfig().getString("model.spark-master")); }

  public RDFSparkGenerationRunner(String sparkMaster) {
    this.sparkMaster = Preconditions.checkNotNull(sparkMaster);
  }

  @Override
  protected void runSteps() throws IOException, JobException, InterruptedException {
    String instanceDir = getInstanceDir();
    int generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);

    RDFModelBuilder modelBuilder = new RDFModelBuilder();
    RDFJobStepConfig jobStepConfig = new RDFJobStepConfig(
        getInstanceDir(),
        getGenerationID(),
        getLastGenerationID(),
        0);
    SparkConf sparkConf = new SparkConf()
        .set("spark.executor.memory", "2g")
        .set("spark.storage.memoryFraction", "0.4");
    JavaSparkContext jsc = new JavaSparkContext(sparkMaster, "RDF", sparkConf);
    String[] jars = JavaSparkContext.jarOfClass(this.getClass());
    if (jars != null && jars.length > 0) {
      for (String jar : jars) {
        jsc.addJar(jar);
      }
    }
    SparkPipeline sp = new SparkPipeline(jsc, "RDF");
    Configuration conf = sp.getConfiguration();
    copyConf(OryxConfiguration.get(), conf);
    conf.set(CONFIG_SERIALIZATION_KEY, ConfigUtils.getDefaultConfig().root().render());
    modelBuilder.build((sp.readTextFile(generationPrefix + "inbound/")), jobStepConfig);

    sp.done();
    jsc.stop();
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
