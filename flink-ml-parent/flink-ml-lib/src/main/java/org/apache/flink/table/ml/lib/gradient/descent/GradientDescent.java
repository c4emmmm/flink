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

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.common.functions.vector.VectorAvg;
import org.apache.flink.table.ml.lib.common.params.column.WithFeatureCol;
import org.apache.flink.table.ml.lib.common.params.column.WithLabelCol;
import org.apache.flink.table.ml.lib.common.params.column.WithPredictionCol;
import org.apache.flink.table.ml.lib.util.TableUtil;
import org.apache.flink.types.Row;

import org.apache.commons.math3.linear.ArrayRealVector;

/**
 *
 */
public class GradientDescent implements Estimator<GradientDescent, GradientDescentModel>,
	WithGradientDescentParams<GradientDescent>,
	WithFeatureCol<GradientDescent>,
	WithLabelCol<GradientDescent>,
	WithPredictionCol<GradientDescent> {
	private final Params params = new Params();

	@Override
	public GradientDescentModel fit(TableEnvironment tEnv, Table input) {
		TableUtil.assertBatchTableEnvironment(tEnv);
		double[] weight = new double[getDim() + 1];

		String featCol = getFeatureCol();
		String labelCol = getLabelCol();
		double l1Reg = getL1Regularization();
		double l2Reg = getL2Regularization();

		long iter = 0;
		long start = System.currentTimeMillis();
		double learningRate = getInitLearningRate();
		double loss;
		do {
			ComputeGradient gradient =
				new ComputeGradient(getGradientType(), getGradientClass(), weight, learningRate,
					l1Reg, l2Reg);
			String gradientName = "computeGradient_" + System.nanoTime();
			TableUtil.registerTableFunction(tEnv, gradientName, gradient);

			VectorAvg avg = new VectorAvg();
			String avgName = "vecAvg_" + System.nanoTime();
			TableUtil.registerAggregateFunction(tEnv, avgName, avg);

			Table resTable =
				input.joinLateral(
					gradientName + "(" + featCol + ", " + labelCol + ") as (loss, gradient)")
					.select("loss.avg as loss, " + avgName + "(gradient) as " + "gradient");

			Row res;
			try {
				res = TableUtil.toDataSet(tEnv, resTable).collect().get(0);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			loss = (double) res.getField(0);
			ArrayRealVector gradVec = new ArrayRealVector((double[]) res.getField(1));
			weight = new ArrayRealVector(weight).add(gradVec).toArray();

			System.out.println("iter=" + iter + ", loss=" + loss + ", learningRate=" + learningRate
				+ ",  cost=" + (System.currentTimeMillis() - start));

			iter += 1;
			learningRate = getInitLearningRate() / Math.sqrt(iter + 1);
		} while ((getMaxIter() <= 0 || iter < getMaxIter()) &&
			(getTolerance() <= 0 || loss > getTolerance()));

		return new GradientDescentModel().setFeatureCol(getFeatureCol())
			.setPredictionCol(getPredictionCol())
			.setWeights(weight);
	}

	@Override
	public Params getParams() {
		return params;
	}
}
