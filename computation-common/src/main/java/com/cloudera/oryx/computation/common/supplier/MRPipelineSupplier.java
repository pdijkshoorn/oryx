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
package com.cloudera.oryx.computation.common.supplier;

import com.cloudera.oryx.common.servcomp.OryxConfiguration;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MRPipelineSupplier implements PipelineSupplier {
  private static final Logger log = LoggerFactory.getLogger(MRPipelineSupplier.class);

  public static final String CONFIG_SERIALIZATION_KEY = "CONFIG_SERIALIZATION";

  private Configuration baseConf;
  private String appName;
  private Class<?> jarClass;

  public MRPipelineSupplier(Configuration baseConf, String appName, Class<?> jarClass) {
    this.baseConf = baseConf;
    this.appName = appName;
    this.jarClass = jarClass;
  }

  @Override
  public Pipeline get(boolean highMemory) {
    Configuration conf = OryxConfiguration.get(baseConf);

    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);

    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.setClass("mapred.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
    // Set old-style equivalents for Avro/Crunch's benefit
    conf.set("avro.output.codec", "snappy");

    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, true);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, true);
    conf.setBoolean(TTConfig.TT_OUTOFBAND_HEARBEAT, true);
    conf.setInt(MRJobConfig.JVM_NUMTASKS_TORUN, -1);

    //conf.setBoolean("crunch.disable.deep.copy", true);

    Config appConfig = ConfigUtils.getDefaultConfig();

    int mapMemoryMB = appConfig.getInt("computation-layer.mapper-memory-mb");
    log.info("Mapper memory: {}", mapMemoryMB);
    int mapHeapMB = (int) (mapMemoryMB / 1.3); // Matches Hadoop's default
    log.info("Mappers have {}MB heap and can access {}MB RAM", mapHeapMB, mapMemoryMB);
    if (conf.get(MRJobConfig.MAP_JAVA_OPTS) != null) {
      log.info("Overriding previous setting of {}, which was '{}'",
          MRJobConfig.MAP_JAVA_OPTS,
          conf.get(MRJobConfig.MAP_JAVA_OPTS));
    }
    conf.set(MRJobConfig.MAP_JAVA_OPTS,
        "-Xmx" + mapHeapMB + "m -XX:+UseCompressedOops -XX:+UseParallelGC -XX:+UseParallelOldGC");
    log.info("Set {} to '{}'", MRJobConfig.MAP_JAVA_OPTS, conf.get(MRJobConfig.MAP_JAVA_OPTS));
    // See comment below on CM
    conf.setInt("mapreduce.map.java.opts.max.heap", mapHeapMB);

    int reduceMemoryMB = appConfig.getInt("computation-layer.reducer-memory-mb");
    log.info("Reducer memory: {}", reduceMemoryMB);
    if (highMemory) {
      reduceMemoryMB *= appConfig.getInt("computation-layer.worker-high-memory-factor");
      log.info("Increasing {} to {} for high-memory step", MRJobConfig.REDUCE_MEMORY_MB, reduceMemoryMB);
    }
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, reduceMemoryMB);

    int reduceHeapMB = (int) (reduceMemoryMB / 1.3); // Matches Hadoop's default
    log.info("Reducers have {}MB heap and can access {}MB RAM", reduceHeapMB, reduceMemoryMB);
    if (conf.get(MRJobConfig.REDUCE_JAVA_OPTS) != null) {
      log.info("Overriding previous setting of {}, which was '{}'",
          MRJobConfig.REDUCE_JAVA_OPTS,
          conf.get(MRJobConfig.REDUCE_JAVA_OPTS));
    }
    conf.set(MRJobConfig.REDUCE_JAVA_OPTS,
        "-Xmx" + reduceHeapMB + "m -XX:+UseCompressedOops -XX:+UseParallelGC -XX:+UseParallelOldGC");
    log.info("Set {} to '{}'", MRJobConfig.REDUCE_JAVA_OPTS, conf.get(MRJobConfig.REDUCE_JAVA_OPTS));
    // I see this in CM but not in Hadoop docs; probably won't hurt as it's supposed to result in
    // -Xmx appended to opts above, which is at worst redundant
    conf.setInt("mapreduce.reduce.java.opts.max.heap", reduceHeapMB);

    conf.setInt("yarn.scheduler.capacity.minimum-allocation-mb", 128);
    conf.setInt("yarn.app.mapreduce.am.resource.mb", 384);

    // Pass total config state
    conf.set(CONFIG_SERIALIZATION_KEY, ConfigUtils.getDefaultConfig().root().render());

    // Make sure to set any args to conf above this line!

    try {
      Job job = Job.getInstance(conf);

      // Basic File IO settings
      FileInputFormat.setMaxInputSplitSize(job, 1L << 30); // ~1073MB
      FileInputFormat.setMinInputSplitSize(job, 1L << 27); // ~134MB
      SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
      conf = job.getConfiguration();
      log.info("Created pipeline configuration {}", conf);
    } catch (IOException e) {
      log.warn("Could not set FileFormat-specific configuration", e);
    }

    return new MRPipeline(jarClass, appName, conf);
  }
}
