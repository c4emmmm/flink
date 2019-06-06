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

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.ml.lib.common.functions.vector.AXPB;
import org.apache.flink.table.ml.lib.common.functions.vector.MatrixSum;
import org.apache.flink.table.ml.lib.common.functions.vector.VectorDotProduct;
import org.apache.flink.table.ml.lib.common.functions.vector.VectorOuterProduct;
import org.apache.flink.table.ml.lib.common.functions.vector.VectorSum;
import org.apache.flink.table.ml.lib.util.TableUtil;
import org.apache.flink.types.Row;

/**
 *
 */
public class ALS implements Estimator<ALS, ALSModel>, WithALSEstimtorParams<ALS> {
	private final Params params = new Params();

	public ALS() {
	}

	@Override
	public ALSModel fit(TableEnvironment tEnv, Table input) {
		TableUtil.assertBatchTableEnvironment(tEnv);
		BatchTableEnvironment bTEnv = (BatchTableEnvironment) tEnv;

		String userCol = getUserCol();
		String itemCol = getItemCol();
		String ratingCol = getLabelCol();
		String modelFilePath = getModelFilePath();

		VectorOuterProduct op = new VectorOuterProduct();
		String opName = "op_" + System.nanoTime();
		tEnv.registerFunction(opName, op);

		VectorDotProduct dp = new VectorDotProduct();
		String dpName = "dp_" + System.nanoTime();
		tEnv.registerFunction(dpName, dp);

		AXPB axpb = new AXPB();
		String axpbName = "axpb_" + System.nanoTime();
		tEnv.registerFunction(axpbName, axpb);

		ComputeFactor computeFactor = new ComputeFactor();
		String computeFactorName = "computeFactor_" + System.nanoTime();
		tEnv.registerFunction(computeFactorName, computeFactor);

		ALSFSPS.writeYFactor(initYFactors(tEnv, input), bTEnv, modelFilePath);

		double predDiff;
		long iter = 0;
		long start = System.currentTimeMillis();
		do {
			Table yFactors = ALSFSPS.getYFactor(tEnv, modelFilePath);

			MatrixSum sumYty = new MatrixSum();
			String sumYtyName = "sumYtyName_" + System.nanoTime();
			TableUtil.registerAggregateFunction(tEnv, sumYtyName, sumYty);

			VectorSum sumYr = new VectorSum();
			String sumYrName = "sumYr_" + System.nanoTime();
			TableUtil.registerAggregateFunction(tEnv, sumYrName, sumYr);

			Table yFactorsWithYty = yFactors.select("y_id, y_fac, " + opName + "(y_fac) as yty");
			Table xFactors = input.join(yFactorsWithYty, itemCol + "===y_id").groupBy(userCol)
				.select(userCol + ", " + sumYtyName + "(yty) as sumYty, "
					+ sumYrName + "(" + axpbName + "(" + ratingCol + ", y_fac, 0.0)) as sumYr")
				.select(userCol + " as x_id, "
					+ computeFactorName + "(sumYty, sumYr, " + getLambda() + ") as x_fac");
			ALSFSPS.writeXFactor(xFactors, bTEnv, modelFilePath);

			MatrixSum sumXtx = new MatrixSum();
			String sumXtxName = "sumXtxName_" + System.nanoTime();
			TableUtil.registerAggregateFunction(tEnv, sumXtxName, sumXtx);

			VectorSum sumXr = new VectorSum();
			String sumXrName = "sumXr_" + System.nanoTime();
			TableUtil.registerAggregateFunction(tEnv, sumXrName, sumXr);

			Table xFactorsWithXtx = xFactors.select("x_id, x_fac, " + opName + "(x_fac) as xtx");
			Table newYFactors = input.join(xFactorsWithXtx, userCol + "===x_id").groupBy(itemCol)
				.select(itemCol + ", " + sumXtxName + "(xtx) as sumXtx, "
					+ sumXrName + "(" + axpbName + "(" + ratingCol + ", x_fac, 0.0)) as sumXr")
				.select(itemCol + " as y_id, "
					+ computeFactorName + "(sumXtx, sumXr, " + getLambda() + ") as y_fac");
			ALSFSPS.writeYFactor(newYFactors, bTEnv, modelFilePath);

			Table diffTable =
				input.join(ALSFSPS.getXFactor(tEnv, modelFilePath), userCol + "===x_id")
					.join(ALSFSPS.getYFactor(tEnv, modelFilePath), itemCol + "===y_id")
					.select("(" + ratingCol + " - " + dpName + "(x_fac, y_fac)).abs as diff")
					.select("diff.max as max_diff");

			try {
				predDiff =
					(double) bTEnv.toDataSet(diffTable, Row.class).collect().get(0).getField(0);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			iter += 1;
			System.out.println("iter=" + iter + ", diff=" + predDiff + ", costMs=" +
				(System.currentTimeMillis() - start));
		} while ((getMaxIter() <= 0 || iter < getMaxIter()) && predDiff > getTolerance());

		return new ALSModel().setUserCol(getUserCol()).setItemCol(getItemCol())
			.setPredictionCol(getPredictionCol())
			.setModelFilePath(getModelFilePath());
	}

	private Table initYFactors(TableEnvironment tEnv, Table input) {
		String itemCol = getItemCol();

		RandomFactor random = new RandomFactor(getRank());
		String funcName = "randomFactor_" + System.nanoTime();
		tEnv.registerFunction(funcName, random);

		return input.select(itemCol + " as y_id").distinct()
			.select("y_id, " + funcName + "() as y_fac");
	}

	@Override
	public Params getParams() {
		return params;
	}
}
