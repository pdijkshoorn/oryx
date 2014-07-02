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

package com.cloudera.oryx.als.computation.popular;

import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public final class PopularMapFn extends OryxDoFn<Pair<Long, LongFloatMap>, Pair<Integer, LongSet>> {

  private static final Partitioner<Long,?> HASHER = new HashPartitioner<Long, Object>();
  private static final int CHUNK_SIZE = 4096;

  private int numReducers;

  @Override
  public void initialize() {
    super.initialize();
    this.numReducers = getContext().getNumReduceTasks();
  }

  @Override
  public void process(Pair<Long, LongFloatMap> input, Emitter<Pair<Integer, LongSet>> emitter) {
    LongFloatMap vector = input.second();
    Preconditions.checkState(!vector.isEmpty());
    // vector is already large to have in memory, and a copy of its key set, copied several times through
    // Crunch / Avro, can exhaust memory. Output in chunks.
    LongSet targetIDs = new LongSet();
    // Make sure we use exactly the same hash:
    int partition = HASHER.getPartition(input.first(), null, numReducers);
    LongPrimitiveIterator it = vector.keySetIterator();
    while (it.hasNext()) {
      targetIDs.add(it.nextLong());
      if (targetIDs.size() >= CHUNK_SIZE) {
        emitter.emit(Pair.of(partition, targetIDs));
        targetIDs = new LongSet();
      }
    }
    if (!targetIDs.isEmpty()) {
      emitter.emit(Pair.of(partition, targetIDs));
    }
  }

}
