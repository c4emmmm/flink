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

package org.apache.flink.table.ml.lib.scale;

import org.apache.flink.table.functions.ScalarFunction;

/**
 *
 */
public class ScaleFunction extends ScalarFunction {

	private final double multiplier;
	private final double inMin;
	private final double outMin;

	public ScaleFunction(double inMin, double inMax, double outMin, double outMax) {
		this.multiplier = (outMax - outMin) / (inMax - inMin);
		this.inMin = inMin;
		this.outMin = outMin;
	}

	public double eval(String value) {
		try {
			return eval(Double.parseDouble(value));
		} catch (NumberFormatException | NullPointerException e) {
			return outMin;
		}
	}

	public double eval(double input) {
		return (input - inMin) * multiplier + outMin;
	}
}
