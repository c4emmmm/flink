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

package org.apache.flink.table.ml.lib.als;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.common.functions.vector.VectorDotProduct;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;

/**
 *
 */
public class ALSModel implements Model<ALSModel>, WithALSBaseParams<ALSModel> {
	private final Params params = new Params();

	public ALSModel() {
	}

	public Table transform(TableEnvironment tEnv, Table input) {
		String userCol = getUserCol();
		String itemCol = getItemCol();
		String predCol = getPredictionCol();
		String modelFilePath = getModelFilePath();

		Table xFactors = ALSFSPS.getXFactor(tEnv, modelFilePath);
		Table yFactors = ALSFSPS.getYFactor(tEnv, modelFilePath);

		Table inputWithFactors = input.join(xFactors, userCol + "===x_id")
			.join(yFactors, itemCol + "===y_id");

		VectorDotProduct dotProduct = new VectorDotProduct();
		String funcName = "dotProduct_" + System.nanoTime();
		tEnv.registerFunction(funcName, dotProduct);

		String[] inputFields = input.getSchema().getFieldNames();
		String predExpr = ExpressionUtil.genAppendUdfCallExpr(funcName, new String[]{"x_fac",
			"y_fac"}, predCol, inputFields);

		return inputWithFactors.select(predExpr);
	}

	@Override
	public Params getParams() {
		return params;
	}
}
