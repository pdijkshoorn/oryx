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
package com.cloudera.oryx.als.computation;

import com.cloudera.oryx.als.computation.types.ALSTypes;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;

public class ALSTypesTest {
  @Test
  public void testFloatArray() throws Exception {
    float[] f = new float[] { 1, 2, 3 };
    ByteBuffer bb = (ByteBuffer) ALSTypes.FLOAT_ARRAY.getOutputMapFn().map(f);
    bb.position(0);
    float[] g = ALSTypes.FLOAT_ARRAY.getInputMapFn().map(bb);
    assertArrayEquals(f, g, 0.01f);
  }
}
