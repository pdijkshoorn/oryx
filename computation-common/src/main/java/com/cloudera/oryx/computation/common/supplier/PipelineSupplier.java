package com.cloudera.oryx.computation.common.supplier;

import org.apache.crunch.Pipeline;

public interface PipelineSupplier {

  Pipeline get(boolean highMemory);

}
