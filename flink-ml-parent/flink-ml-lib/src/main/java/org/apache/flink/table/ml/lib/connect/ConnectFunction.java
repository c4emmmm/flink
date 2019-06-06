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

package org.apache.flink.table.ml.lib.connect;

import org.apache.flink.table.functions.ScalarFunction;

/**
 *
 */
public class ConnectFunction extends ScalarFunction {
	private final int totalDim;
	private final boolean[] arrayFlag;

	public ConnectFunction(int totalDim, boolean[] arrayFlag) {
		this.totalDim = totalDim;
		this.arrayFlag = arrayFlag;
	}

	public double[] eval(Object... input) {
		double[] data = new double[totalDim];
		int i = 0;
		for (int j = 0; j < input.length; j++) {
			Object d = input[j];
			if (arrayFlag[j]) {
				for (double v : (double[]) d) {
					data[i++] = v;
				}
			} else {
				data[i++] = (double) d;
			}
		}
		return data;
	}
}
