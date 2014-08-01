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
package com.cloudera.oryx.rdf.computation.modelbuilder;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.modelbuilder.AbstractModelBuilder;
import com.cloudera.oryx.rdf.computation.RDFJobStepConfig;
import com.cloudera.oryx.rdf.computation.build.BuildTreeFn;
import com.cloudera.oryx.rdf.computation.build.DistributeExampleFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class RDFModelBuilder extends AbstractModelBuilder<String, String, RDFJobStepConfig> {
  @Override
  public String build(PCollection<String> input, RDFJobStepConfig jobStepConfig) throws IOException {
    String instanceDir = jobStepConfig.getInstanceDir();
    int generationID = jobStepConfig.getGenerationID();
    String instancePrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);

    Pipeline p = input.getPipeline();
    Configuration conf = p.getConfiguration();

    input.parallelDo("distributeData", new DistributeExampleFn(), Avros.tableOf(Avros.ints(), Avros.strings()))
        .groupByKey(groupingOptions())
        .parallelDo("buildTrees", new BuildTreeFn(), Avros.strings())
        .write(compressedTextOutput(conf, instancePrefix + "trees/"));
    return instancePrefix;
  }
}
