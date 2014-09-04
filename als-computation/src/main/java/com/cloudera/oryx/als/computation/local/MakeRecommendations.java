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

package com.cloudera.oryx.als.computation.local;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.TopN;
import com.cloudera.oryx.als.computation.recommend.RecommendIterator;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

final class MakeRecommendations implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(MakeRecommendations.class);

  private final File modelDir;
  private final LongObjectMap<LongSet> knownItemIDs;
  private final LongObjectMap<float[]> X;
  private final LongObjectMap<float[]> Y;
  private final StringLongMapping idMapping;

  MakeRecommendations(File modelDir,
                      LongObjectMap<LongSet> knownItemIDs,
                      LongObjectMap<float[]> X,
                      LongObjectMap<float[]> Y,
                      StringLongMapping idMapping) {
    this.modelDir = modelDir;
    this.knownItemIDs = knownItemIDs;
    this.X = X;
    this.Y = Y;
    this.idMapping = idMapping;
  }

  @Override
  public Object call() throws IOException {

    log.info("Starting recommendations");

    Config config = ConfigUtils.getDefaultConfig();
    final int howMany = config.getInt("model.recommend.how-many");
    
    //Rahim: Users that are available in the users file are those that we want to do recommendation for them.
    final LongSet users = new LongSet();
    final boolean recsForSpecUsers = config.getBoolean("model.recommend.specificUsers");
    if (recsForSpecUsers){
		String usersFile=config.getString("model.recommend.usersFile");	
		try {
		BufferedReader reader = new BufferedReader(new FileReader(usersFile));
		while(true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				users.add(Long.parseLong(line.trim()));
				log.info("User:"+Long.parseLong(line.trim()));
			} 
		reader.close();
		}
	
	catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
    }
    }
    final LongPrimitiveIterator it = X.keySetIterator();

    final File recommendDir = new File(modelDir, "recommend");
    IOUtils.mkdirs(recommendDir);

    ExecutorService executor = ExecutorUtils.buildExecutor("Recommend");
    Collection<Future<Object>> futures = Lists.newArrayList();

    try {
      int numThreads = ExecutorUtils.getParallelism();
      for (int i = 0; i < numThreads; i++) {
        final int workerNumber = i;
        futures.add(executor.submit(new Callable<Object>() {
          @Override
          public Void call() throws IOException {
            Writer out = IOUtils.buildGZIPWriter(new File(recommendDir, workerNumber + ".csv.gz"));
            try {
              while (true) {
                long userID;
                synchronized (it) {
                  if (!it.hasNext()) {
                    return null;
                  }
                  userID = it.nextLong();
                  if (!users.contains(userID) && recsForSpecUsers)
                	  continue;
                }
                float[] userFeatures = X.get(userID);
                LongSet knownItemIDsForUser = knownItemIDs == null ? null : knownItemIDs.get(userID);
                Iterable<NumericIDValue> recs = TopN.selectTopN(
                    new RecommendIterator(userFeatures, Y.entrySet().iterator(), knownItemIDsForUser),
                    howMany);
                String userIDString = idMapping.toString(userID);
                for (NumericIDValue rec : recs) {
                  out.write(DelimitedDataUtils.encode(',',
                                                      userIDString,
                                                      idMapping.toString(rec.getID()),
                                                      Float.toString(rec.getValue())));
                  out.write('\n');
                }
              }
            } finally {
              out.close();
            }
          }
        }));

      }
      ExecutorUtils.getResults(futures);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }

    log.info("Finished recommendations");
    
    
    
    return null;
  }



}
