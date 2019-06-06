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

package org.apache.flink.table.ml.lib.sampling.random;

import org.apache.flink.table.functions.ScalarFunction;

/**
 *
 */
public class RandomMarker extends ScalarFunction {
	private final int lines;

	private int count = 0;
	private int outCount = 0;

	public RandomMarker(int lines) {
		this.lines = lines;
	}

	public long eval() {
		count += 1;
		if (Math.random() <= 1.0 * lines / count) {
			outCount += 1;
			return outCount;
		} else {
			return 0;
		}
	}
}
