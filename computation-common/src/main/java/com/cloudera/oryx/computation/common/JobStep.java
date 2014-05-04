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

package com.cloudera.oryx.computation.common;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import com.cloudera.oryx.computation.common.supplier.MRPipelineSupplier;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.OryxConfiguration;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * Abstract superclass of all "steps".
 *
 * @author Sean Owen
 */
public abstract class JobStep extends Configured implements Tool, HasState {

  private static final Logger log = LoggerFactory.getLogger(JobStep.class);

  public static final String CONFIG_SERIALIZATION_KEY = "CONFIG_SERIALIZATION";

  private JobStepConfig config;
  private Date startTime;
  private Date endTime;
  private MRPipelineExecution exec;

  protected abstract JobStepConfig parseConfig(String[] args);

  protected JobStepConfig getConfig() {
    return config;
  }

  @Override
  public final Collection<StepState> getStepStates() throws IOException, InterruptedException {
    String name = getCustomJobName();
    StepStatus status = determineStatus();
    StepState state = new StepState(startTime, endTime, name, status);
    if (status == StepStatus.COMPLETED) {
      // This completed, -- perhaps without ever running, which means Hadoop doesn't report state because
      // it was skipped. It's complete.
      state.setMapProgress(1.0f);
      state.setReduceProgress(1.0f);
    } else {
      float[] progresses = determineProgresses();
      if (progresses != null) {
        // 0 is setup progress; we don't care
        state.setMapProgress(progresses[1]);
        state.setReduceProgress(progresses[2]);
      }
    }
    return Collections.singletonList(state);
  }

  private StepStatus determineStatus() throws IOException, InterruptedException {
    JobContext job = getJob();
    if (job == null) {
      return StepStatus.COMPLETED;
    }
    Cluster cluster = new Cluster(getConf());
    try {
      JobID jobID = job.getJobID();
      if (jobID == null) {
        return StepStatus.PENDING;
      }
      Job runningJob = cluster.getJob(jobID);
      if (runningJob == null) {
        return StepStatus.PENDING;
      }
      JobStatus.State state = runningJob.getJobState();
      switch (state) {
        case PREP:
          return StepStatus.PENDING;
        case RUNNING:
          return StepStatus.RUNNING;
        case SUCCEEDED:
          return StepStatus.COMPLETED;
        case FAILED:
          return StepStatus.FAILED;
        case KILLED:
          return StepStatus.CANCELLED;
      }
      throw new IllegalArgumentException("Unknown Hadoop job state " + state);
    } finally {
      cluster.close();
    }
  }

  private JobContext getJob() {
    return exec == null ? null : exec.getJobs().get(0).getJob();
  }

  /**
   * @return three progress values, in [0,1], as a {@code float[]}, representing setup, mapper and reducer progress
   */
  private float[] determineProgresses() throws IOException, InterruptedException {
    if (exec == null) {
      return null;
    }
    Cluster cluster = new Cluster(getConf());
    try {
      JobID jobID = getJob().getJobID();
      if (jobID == null) {
        return null;
      }
      Job runningJob = cluster.getJob(jobID);
      if (runningJob == null) {
        return null;
      }

      return new float[] { runningJob.setupProgress(), runningJob.mapProgress(), runningJob.reduceProgress() };
    } finally {
      cluster.close();
    }
  }

  @Override
  public final int run(String[] args) throws InterruptedException, IOException, JobException {

    MemoryHandler.setSensibleLogFormat();

    if (log.isInfoEnabled()) {
      log.info("args are {}", Arrays.toString(args));
    }

    config = parseConfig(args);

    String name = getCustomJobName();
    log.info("Running {}", name);

    long start = System.currentTimeMillis();
    startTime = new Date(start);
    MRPipeline pipeline = createPipeline();
    try {
      if (pipeline == null) {
        log.info("{} does not need to run", name);
      } else {

        exec = pipeline.runAsync();
        log.info("Waiting for {} to complete", name);
        exec.waitUntilDone();

        PipelineExecution.Status exitStatus = exec.getStatus();
        if (exitStatus == PipelineExecution.Status.KILLED || exitStatus == PipelineExecution.Status.FAILED) {
          exec.kill();
          throw new JobException(name + " failed in state " + exitStatus);
        }

        log.info("Finished {}", name);
        postRun();

      }
    } catch (InterruptedException ie) {
      log.warn("Interrupted {}", name);
      exec.kill();
      throw ie;
    } finally {
      long end = System.currentTimeMillis();
      endTime = new Date(end);
      log.info("Completed {} in {}s", this, (end - start) / 1000L);
      if (pipeline != null) {
        pipeline.done();
      }
    }

    return 0;
  }

  /**
   * Subclasses override this to make the {@link MRPipeline}.
   *
   * @return {@link MRPipeline} encapsulating the MapReduce to run for this step or null if there is nothing
   *  to run
   */
  protected MRPipeline createPipeline() throws IOException {
    return null;
  }

  /**
   * Subclasses override this to perform any steps after the job has finished.
   */
  protected void postRun() throws IOException {
    // do nothing
  }

  protected boolean isHighMemoryStep() {
    return false;
  }

  /**
   * Creates a new {@link MRPipeline} instance that contains common configuration
   * settings.
   *
   * @return a new {@link MRPipeline} instance, suitably configured
   */
  protected MRPipeline createBasicPipeline(Class<?> jarClass) throws IOException {
    return (MRPipeline) new MRPipelineSupplier(getConf(), getCustomJobName(), jarClass).get(isHighMemoryStep());
  }

  protected final boolean validOutputPath(String outputPathKey) throws IOException {
    Preconditions.checkArgument(outputPathKey != null && outputPathKey.endsWith("/"),
        "%s should end with /", outputPathKey);

    String jobName = getCustomJobName();

    Store store = Store.get();
    if (store.exists(outputPathKey, false)) {
      log.info("Output path {} already exists", outputPathKey);
      if (store.exists(outputPathKey + "_SUCCESS", true)) {
        log.info("{} shows _SUCCESS present; skipping", jobName);
        return false;
      }
      log.info("{} seems to have stopped during an earlier run; deleting output at {} and recomputing",
          jobName, outputPathKey);
      store.recursiveDelete(outputPathKey);
    } else {
      log.info("Output path is clear for writing: {}", outputPathKey);
    }
    return true;
  }

  protected final <T> Source<T> input(String inputPathKey, PType<T> ptype) {
    return avroInput(inputPathKey, ptype);
  }

  protected final <T> Source<T> avroInput(String inputPathKey, PType<T> avroType) {
    return From.avroFile(Namespaces.toPath(inputPathKey), (AvroType<T>) avroType);
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

  public static void run(Tool step, String[] args) throws IOException, InterruptedException, JobException {
    log.info("Running step {}", step.getClass().getSimpleName());
    try {
      int result = ToolRunner.run(OryxConfiguration.get(), step, args);
      if (result != 0) {
        throw new JobException(step + " hasd bad exit status: " + result);
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (JobException je) {
      throw je;
    } catch (Exception e) {
      throw new JobException(e);
    }
  }

  protected final String defaultCustomJobName() {
    return "Oryx-" + config.getInstanceDir() + '-' + config.getGenerationID() + '-' + getClass().getSimpleName();
  }

  protected String getCustomJobName() {
    return defaultCustomJobName();
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName();
  }

}
