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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import org.apache.commons.math3.linear.ArrayRealVector;

/**
 *
 */
public class ComputeGradient extends TableFunction<Tuple2<Double, double[]>> {
	private final Gradient.Type gradientType;
	private final String gradientClass;
	private final double[] weight;
	private final double learningRate;
	private final double l1Reg;
	private final double l2Reg;

	private Gradient gradient;

	public ComputeGradient(Gradient.Type gradientType, String gradientClass, double[] weight, double learningRate, double l1Reg, double l2Reg) {
		this.gradientType = gradientType;
		this.gradientClass = gradientClass;
		this.weight = weight;
		this.learningRate = learningRate;
		this.l1Reg = l1Reg;
		this.l2Reg = l2Reg;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		switch (gradientType) {
			case LEAST_SQUARE:
				gradient = new Gradient.LeastSquaresGradient();
				break;
			case LOGISTIC:
				gradient = new Gradient.LogisticGradient();
				break;
			case CUSTOMIZE:
				gradient = (Gradient) Class.forName(gradientClass).newInstance();
				break;
			default:
				throw new RuntimeException("Unsupported gradient type:" + gradientType);
		}
	}

	public void eval(double[] feat, double label) {
		double[] featWithIntercept = new double[feat.length + 1];
		System.arraycopy(feat, 0, featWithIntercept, 0, feat.length);
		featWithIntercept[feat.length] = 1.0;
		Tuple2<Double, double[]> lossGradient = gradient.lossGradient(featWithIntercept, label,
			weight);

		double loss = lossGradient.f0;
		ArrayRealVector gradVec = new ArrayRealVector(lossGradient.f1);

		if (l2Reg > 0) {
			gradVec = gradVec.add(new ArrayRealVector(weight).mapMultiply(l2Reg));
		}
		if (l1Reg > 0) {
			double[] l1RegVec = new double[weight.length];
			for (int i = 0; i < weight.length; i++) {
				double w = weight[i];
				l1RegVec[i] = w > 0 ? l1Reg : w == 0 ? 0 : -l1Reg;
			}
			gradVec = gradVec.add(new ArrayRealVector(l1RegVec));
		}
		gradVec.mapMultiplyToSelf(-learningRate);

		collect(new Tuple2<>(loss, gradVec.toArray()));
	}
}
