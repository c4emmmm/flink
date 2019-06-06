/*
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

package org.apache.flink.table.ml.lib.kmeans;

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.common.functions.vector.VectorAvg;
import org.apache.flink.table.ml.lib.sampling.random.RandomSampling;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;
import org.apache.flink.table.ml.lib.util.TableUtil;
import org.apache.flink.table.ml.lib.util.VectorUtil;

/**
 *
 */
public class KMeans implements Estimator<KMeans, KMeansModel>, WithKMeansParams<KMeans> {
	private Params params = new Params();

	@Override
	public KMeansModel fit(TableEnvironment tEnv, Table input) {
		TableUtil.assertBatchTableEnvironment(tEnv);

		String inputCol = getInputCol();

		double[][] centroids = toCentroids(tEnv, initRandom(tEnv, input), inputCol);
		double[][] previousCentroids;

		long iter = 0;
		double diff;
		long start = System.currentTimeMillis();
		do {
			FindClosest findClosest = new FindClosest(centroids);
			String findClosestUniqName = "findClosest_" + System.currentTimeMillis();
			tEnv.registerFunction(findClosestUniqName, findClosest);

			String computeClusterExpr = ExpressionUtil
				.genAppendUdfCallExpr(findClosestUniqName, inputCol, "kmeans_idx", inputCol);
			Table inputWithCluster = input.select(computeClusterExpr);

			VectorAvg avg = new VectorAvg();
			String avgUniqName = "vectorAvg_" + System.currentTimeMillis();
			TableUtil.registerAggregateFunction(tEnv, avgUniqName, avg);

			String computeCentoridsExpr = ExpressionUtil
				.genAppendUdfCallExpr(avgUniqName, inputCol, "centroids", "kmeans_idx");
			Table centroidsTable = inputWithCluster.groupBy("kmeans_idx")
				.select(computeCentoridsExpr)
				.orderBy("kmeans_idx");

			previousCentroids = centroids;
			centroids = toCentroids(tEnv, centroidsTable, "centroids");
			iter += 1;

			diff = 0;
			for (int i = 0; i < getK(); i++) {
				double[] curC = centroids[i];
				double[] prevC = previousCentroids[i];
				double distance = VectorUtil.distance(curC, prevC);
				if (distance > diff) {
					diff = distance;
				}
			}

			System.out.println(String.format("iter=%d, diff=%f, cost=%d", iter, diff,
				System.currentTimeMillis() - start));
		} while ((getMaxIter() <= 0 || iter < getMaxIter()) &&
			(getTolerance() <= 0 || diff > getTolerance()) &&
			diff > 0
		);

		return new KMeansModel()
			.setInputCol(getInputCol())
			.setPredictionCol(getPredictionCol())
			.setCentroids(centroids);
	}

	@Override
	public Params getParams() {
		return params;
	}

	private Table initRandom(TableEnvironment tEnv, Table data) {
		return new RandomSampling().setCount(getK()).transform(tEnv, data);
	}

	private double[][] toCentroids(TableEnvironment tEnv, Table centroidsTable, String field) {
		try {
			return TableUtil.toDataSet(tEnv, centroidsTable.select(field)).collect()
				.stream().map((r) -> (double[]) r.getField(0)).toArray(double[][]::new);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
