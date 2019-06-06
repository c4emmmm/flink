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

package org.apache.flink.table.ml.lib.gradient.descent;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.common.params.column.WithFeatureCol;
import org.apache.flink.table.ml.lib.common.params.column.WithPredictionCol;

/**
 *
 */
public class GradientDescentModel implements Model<GradientDescentModel>,
	WithGradientDescentWeights<GradientDescentModel>,
	WithFeatureCol<GradientDescentModel>,
	WithPredictionCol<GradientDescentModel> {
	private final Params params = new Params();

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		double[] weightWithIntercept = getWeights();
		double[] weight = new double[weightWithIntercept.length - 1];
		System.arraycopy(weightWithIntercept, 0, weight, 0, weight.length);
		double interceptWeight = weightWithIntercept[weight.length];

		VectorDotProductWithFixedY dot = new VectorDotProductWithFixedY(weight);
		String funcName = "dotProduct_" + System.nanoTime();
		tEnv.registerFunction(funcName, dot);

		String featCol = getFeatureCol();
		String predCol = getPredictionCol();
		String[] inputFields = input.getSchema().getFieldNames();

		String dotExpr =
			"(" + funcName + "(" + featCol + ") + " + interceptWeight + ") as " + predCol;
		StringBuilder selectExpr = new StringBuilder(dotExpr);
		for (String f : inputFields) {
			if (!f.equals(predCol)) {
				selectExpr.append(", ").append(f);
			}
		}

		return input.select(selectExpr.toString());
	}

	@Override
	public Params getParams() {
		return params;
	}
}
