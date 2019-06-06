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

package org.apache.flink.table.ml.lib.common.functions.vector;

import org.apache.flink.table.functions.AggregateFunction;

import org.apache.commons.math3.linear.ArrayRealVector;

/**
 *
 */
public class VectorSum extends AggregateFunction<double[], VectorSum.VectorSumAcc> {

	@Override
	public double[] getValue(VectorSumAcc acc) {
		return new ArrayRealVector(acc.sum).toArray();
	}

	@Override
	public VectorSumAcc createAccumulator() {
		return new VectorSumAcc();
	}

	public void accumulate(VectorSumAcc acc, double[] x) {
		if (acc.sum == null) {
			acc.sum = x;
		} else {
			acc.sum = new ArrayRealVector(acc.sum).add(new ArrayRealVector(x)).toArray();
		}
	}

	public void merge(VectorSumAcc acc, Iterable<VectorSumAcc> others) {
		ArrayRealVector accSum = null;
		if (acc.sum != null) {
			accSum = new ArrayRealVector(acc.sum);
		}

		for (VectorSumAcc o : others) {
			if (acc.sum == null && o.sum != null) {
				acc.sum = o.sum;
				accSum = new ArrayRealVector(o.sum);
			} else if (o.sum != null) {
				accSum.add(new ArrayRealVector(o.sum));
			}
		}
	}

	public void resetAccumulator(VectorSumAcc acc) {
		acc.sum = null;
	}

	/**
	 *
	 */
	public class VectorSumAcc {
		double[] sum;
	}
}
