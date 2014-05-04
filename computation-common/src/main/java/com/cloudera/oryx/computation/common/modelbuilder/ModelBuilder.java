package com.cloudera.oryx.computation.common.modelbuilder;

import com.cloudera.oryx.computation.common.JobStepConfig;
import org.apache.crunch.PCollection;

import java.io.IOException;

public interface ModelBuilder<Input, Output, Conf extends JobStepConfig> {
  Output build(PCollection<Input> input, Conf conf) throws IOException;
}
