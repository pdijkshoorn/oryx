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

package com.cloudera.oryx.als.computation.types;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongSet;

import com.google.common.collect.Lists;
import org.apache.crunch.Pair;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;

public final class ALSTypes {

  public static final PType<Integer> INTS = Avros.ints();

  public static final PType<Long> LONGS = Avros.longs();

  public static final PType<float[]> FLOAT_ARRAY = Avros.derivedImmutable(float[].class,
       new MapFn<Collection<Float>, float[]>() {
         @Override
         public float[] map(Collection<Float> input) {
           List<Float> in = (List) input;
           float[] ret = new float[input.size()];
           for (int i = 0; i < ret.length; i++) {
             ret[i] = in.get(i);
           }
           return ret;
         }
       },
       new MapFn<float[], Collection<Float>>() {
         @Override
         public Collection<Float> map(final float[] input) {
           return new AbstractList<Float>() {
             public int size() { return input.length; }
             public Float get(int i) { return input[i]; }
           };
         };
       }, Avros.collections(Avros.floats()));

  public static final PType<NumericIDValue> IDVALUE = Avros.derivedImmutable(NumericIDValue.class,
      new MapFn<Pair<Long, Float>, NumericIDValue>() { public NumericIDValue map(Pair<Long, Float> in) {
        return new NumericIDValue(in.first(), in.second());
      }},
      new MapFn<NumericIDValue, Pair<Long, Float>>() { public Pair<Long, Float> map(NumericIDValue in) {
        return Pair.of(in.getID(), in.getValue());
      }},
      Avros.pairs(Avros.longs(), Avros.floats()));

  public static final PType<LongSet> ID_SET = Avros.derivedImmutable(LongSet.class,
      new MapFn<Collection<Long>, LongSet>() {
        @Override
        public LongSet map(Collection<Long> input) {
          LongSet set = new LongSet(input.size());
          for (long l : input) {
            set.add(l);
          }
          return set;
        }
      },
      new MapFn<LongSet, Collection<Long>>() {
        @Override
        public Collection<Long> map(LongSet input) {
          Collection<Long> collection = Lists.newArrayListWithCapacity(input.size());
          for (Long l : input) {
            collection.add(l);
          }
          return collection;
        }
      }, Avros.collections(LONGS));

  public static final PType<LongFloatMap> ID_FLOAT_MAP = Avros.derivedImmutable(LongFloatMap.class,
      new MapFn<Collection<Pair<Long,Float>>, LongFloatMap>() {
        @Override
        public LongFloatMap map(Collection<Pair<Long,Float>> input) {
          LongFloatMap map = new LongFloatMap(input.size());
          for (Pair<Long,Float> pair : input) {
            map.put(pair.first(), pair.second());
          }
          return map;
        }
      },
      new MapFn<LongFloatMap, Collection<Pair<Long,Float>>>() {
        @Override
        public Collection<Pair<Long,Float>> map(LongFloatMap input) {
          Collection<Pair<Long,Float>> collection = Lists.newArrayListWithCapacity(input.size());
          for (LongFloatMap.MapEntry entry : input.entrySet()) {
            collection.add(Pair.of(entry.getKey(), entry.getValue()));
          }
          return collection;
        }
      }, Avros.collections(Avros.pairs(LONGS, Avros.floats())));

  public static final PType<Pair<Long, NumericIDValue>> VALUE_MATRIX = Avros.pairs(LONGS, IDVALUE);

  public static final PType<MatrixRow> DENSE_ROW_MATRIX = Avros.derived(MatrixRow.class,
      new MapFn<Pair<Long, float[]>, MatrixRow>() {
        @Override
        public MatrixRow map(Pair<Long, float[]> input) {
          return new MatrixRow(input.first(), input.second());
        }
      },
      new MapFn<MatrixRow, Pair<Long, float[]>>() {
        @Override
        public Pair<Long, float[]> map(MatrixRow input) {
          return Pair.of(input.getRowId(), input.getValues());
        }
      },
      Avros.pairs(Avros.longs(), FLOAT_ARRAY));

  public static final PType<Pair<Long, LongFloatMap>> SPARSE_ROW_MATRIX = Avros.pairs(LONGS, ID_FLOAT_MAP);

  public static final PType<Pair<Integer, Pair<Long, Pair<float[], LongSet>>>> REC_TYPE =
      Avros.pairs(INTS, Avros.pairs(LONGS, Avros.pairs(FLOAT_ARRAY, ID_SET)));

  private ALSTypes() {}
}
