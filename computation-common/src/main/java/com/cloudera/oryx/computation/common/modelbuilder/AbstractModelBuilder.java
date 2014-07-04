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
package com.cloudera.oryx.computation.common.modelbuilder;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.typesafe.config.Config;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.io.At;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public abstract class AbstractModelBuilder<Input, Output, Conf extends JobStepConfig>
    implements ModelBuilder<Input, Output, Conf> {

  protected final Config getConfig() { return ConfigUtils.getDefaultConfig(); }

  protected final <T> Source<T> input(String inputPathKey, PType<T> ptype) {
    return avroInput(inputPathKey, ptype);
  }

  protected final <T> Source<T> avroInput(String inputPathKey, PType<T> avroType) {
    return From.avroFile(Namespaces.toPath(inputPathKey), avroType);
  }

  protected final Source<String> textInput(String inputPathKey) {
    return From.textFile(Namespaces.toPath(inputPathKey));
  }

  protected final Target avroOutput(String outputPathKey) {
    return To.avroFile(Namespaces.toPath(outputPathKey));
  }

  protected final Target output(String outputPathKey) {
    return avroOutput(outputPathKey);
  }

  protected final Target compressedTextOutput(Configuration conf, String outputPathKey) {
    // The way this is used, it doesn't seem like we can just set the object in getConf(). Need
    // to set the copy in the MRPipeline directly?
    conf.setBoolean("mapred.output.compress", true);
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
    conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
    return To.textFile(Namespaces.toPath(outputPathKey));
  }

  protected final GroupingOptions groupingOptions() {
    return groupWithComparator(null);
  }

  protected final GroupingOptions groupWithComparator(
      Class<? extends RawComparator<?>> comparator) {
    return groupingOptions(HashPartitioner.class, comparator);
  }

  protected final GroupingOptions groupingOptions(
      Class<? extends Partitioner> partitionerClass,
      Class<? extends RawComparator<?>> comparator) {
    return groupingOptions(partitionerClass, comparator, comparator);
  }

  protected final GroupingOptions groupingOptions(
      Class<? extends Partitioner> partitionerClass,
      Class<? extends RawComparator<?>> groupingComparator,
      Class<? extends RawComparator<?>> sortComparator) {
    GroupingOptions.Builder b = GroupingOptions.builder()
        .partitionerClass(partitionerClass)
        .numReducers(getNumReducers());

    if (groupingComparator != null) {
      b.groupingComparatorClass(groupingComparator);
    }
    if (sortComparator != null) {
      b.sortComparatorClass(sortComparator);
    }
    return b.build();
  }

  protected final int getNumReducers() {
    String parallelismString = ConfigUtils.getDefaultConfig().getString("computation-layer.parallelism");
    if ("auto".equals(parallelismString)) {
      return 11; // Default to a prime. Helps avoid problems with funny distributions of IDs.
    } else {
      return Integer.parseInt(parallelismString);
    }
  }
}
