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

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Random;

/**
 *
 */
public class RandomFactor extends ScalarFunction {
	private final int dim;
	private final Random rand = new Random();

	public RandomFactor(int dim) {
		this.dim = dim;
	}

	public double[] eval() {
		double[] arr = new double[dim];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = rand.nextDouble();
		}
		return arr;
	}

	@Override
	public boolean isDeterministic() {
		return false;
	}
}
