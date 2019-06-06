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
public class VectorAvg extends AggregateFunction<double[], VectorAvg.VectorAvgAcc> {

	@Override
	public double[] getValue(VectorAvgAcc acc) {
		return new ArrayRealVector(acc.sum).mapDivide(acc.count).toArray();
	}

	@Override
	public VectorAvgAcc createAccumulator() {
		return new VectorAvgAcc();
	}

	public void accumulate(VectorAvgAcc acc, double[] x) {
		if (acc.sum == null) {
			acc.sum = x;
		} else {
			acc.sum = new ArrayRealVector(acc.sum).add(new ArrayRealVector(x)).toArray();
			acc.count += 1;
		}
	}

	public void merge(VectorAvgAcc acc, Iterable<VectorAvgAcc> others) {
		ArrayRealVector accSum = null;
		if (acc.sum != null) {
			accSum = new ArrayRealVector(acc.sum);
		}

		for (VectorAvgAcc o : others) {
			if (acc.sum == null && o.sum != null) {
				acc.sum = o.sum;
				acc.count = o.count;
				accSum = new ArrayRealVector(o.sum);
			} else if (o.sum != null) {
				accSum.add(new ArrayRealVector(o.sum));
				acc.count += o.count;
			}
		}
	}

	public void resetAccumulator(VectorAvgAcc acc) {
		acc.sum = null;
		acc.count = 0;
	}

	/**
	 *
	 */
	public class VectorAvgAcc {
		double[] sum;
		long count = 0;
	}
}
