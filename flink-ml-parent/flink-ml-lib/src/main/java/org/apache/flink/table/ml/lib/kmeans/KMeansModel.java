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

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.common.params.column.WithInputCol;
import org.apache.flink.table.ml.lib.common.params.column.WithPredictionCol;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;

/**
 *
 */
public class KMeansModel implements Model<KMeansModel>, WithInputCol<KMeansModel>,
	WithPredictionCol<KMeansModel>, WithKMeansCentroids<KMeansModel> {
	private Params params = new Params();

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		String[] inputFields = input.getSchema().getFieldNames();
		String inputCol = getInputCol();
		String predCol = getPredictionCol();
		double[][] centroids = getCentroids();

		FindClosest findClosest = new FindClosest(centroids);
		String funcUniqName = "findClosest_" + System.currentTimeMillis();
		tEnv.registerFunction(funcUniqName, findClosest);

		String computeClusterExpr = ExpressionUtil
			.genAppendUdfCallExpr(funcUniqName, inputCol, predCol, inputFields);
		return input.select(computeClusterExpr);
	}

	@Override
	public Params getParams() {
		return params;
	}
}
