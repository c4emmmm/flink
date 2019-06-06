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

import org.apache.commons.math3.linear.ArrayRealVector;

/**
 *
 */
public interface Gradient {

	Tuple2<Double, double[]> lossGradient(double[] data, double label, double[] weight);

	/**
	 *
	 */
	enum Type {
		LEAST_SQUARE, LOGISTIC, CUSTOMIZE
	}

	/**
	 *
	 */
	class LeastSquaresGradient implements Gradient {

		@Override
		public Tuple2<Double, double[]> lossGradient(double[] data, double label, double[] weight) {
			ArrayRealVector dataVec = new ArrayRealVector(data);
			double diff = dataVec.dotProduct(new ArrayRealVector(weight)) - label;
			double loss = diff * diff / 2;
			double[] grad = dataVec.mapMultiply(diff).toArray();
			return new Tuple2<>(loss, grad);
		}
	}

	/**
	 *
	 */
	class LogisticGradient implements Gradient {

		@Override
		public Tuple2<Double, double[]> lossGradient(double[] data, double label, double[] weight) {
			ArrayRealVector dataVec = new ArrayRealVector(data);
			double margin = -1.0 * dataVec.dotProduct(new ArrayRealVector(weight));
			double multiplier = (1.0 / (1.0 + Math.exp(margin))) - label;
			double[] grad = dataVec.mapMultiply(multiplier).toArray();
			double loss = label > 0 ? log1pExp(margin) : log1pExp(margin) - margin;
			return new Tuple2<>(loss, grad);
		}

		private double log1pExp(double x) {
			if (x > 0) {
				return x + Math.log1p(Math.exp(-x));
			} else {
				return Math.log1p(Math.exp(x));
			}
		}
	}
}
