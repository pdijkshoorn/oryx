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

package com.cloudera.oryx.als.computation.iterate.row;

import com.cloudera.oryx.als.computation.ComputationDataUtils;
import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public final class YState implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(YState.class);

  private final PType<MatrixRow> ptype;
  private final String popularKey;
  private final String yKeyKey;

  private transient LongObjectMap<float[]> Y;
  private transient RealMatrix YTY;

  public YState(PType<MatrixRow> ptype) {
    this(ptype, null, null);
  }

  public YState(PType<MatrixRow> ptype, String popularKey, String yKeyKey) {
    this.ptype = ptype;
    this.popularKey = popularKey;
    this.yKeyKey = yKeyKey;
  }

  synchronized void initialize(TaskInputOutputContext<?,?,?,?> context,
                               int currentPartition,
                               int numPartitions) {
    if (YTY != null) {
      // Already initialized
      return;
    }

    Configuration conf = context.getConfiguration();
    ptype.initialize(conf);

    LongSet expectedIDs;
    try {
      expectedIDs = ComputationDataUtils.readExpectedIDsFromPartition(
          currentPartition,
          numPartitions,
          popularKey == null ? conf.get(RowStep.POPULAR_KEY) : popularKey,
          context,
          conf);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    String yKey = yKeyKey == null ? conf.get(RowStep.Y_KEY_KEY) : yKeyKey;
    log.info("Reading X or Y from {}", yKey);

    Y = new LongObjectMap<float[]>();

    Iterable<MatrixRow> in;
    try {
      in = new AvroFileSource<MatrixRow>(Namespaces.toPath(yKey), (AvroType<MatrixRow>) ptype).read(conf);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    RealMatrix theYTY = null;
    int dimension = 0;
    long count = 0;
    for (MatrixRow record : in) {
      long keyID = record.getRowId();
      float[] vector = record.getValues();
      Preconditions.checkNotNull(vector, "Vector was null for %s?", keyID);

      if (theYTY == null) {
        dimension = vector.length;
        theYTY = new Array2DRowRealMatrix(dimension, dimension);
      }
      for (int row = 0; row < dimension; row++) {
        double rowValue = vector[row];
        for (int col = 0; col < dimension; col++) {
          theYTY.addToEntry(row, col, rowValue * vector[col]);
        }
      }

      if (expectedIDs == null || expectedIDs.contains(keyID)) {
        Y.put(keyID, vector);
      }

      if (++count % 1000 == 0) {
        context.progress();
      }
    }

    Preconditions.checkNotNull(theYTY);
    YTY = theYTY;
  }

  public LongObjectMap<float[]> getY() {
    return Y;
  }

  public RealMatrix getYTY() {
    return YTY;
  }

}
