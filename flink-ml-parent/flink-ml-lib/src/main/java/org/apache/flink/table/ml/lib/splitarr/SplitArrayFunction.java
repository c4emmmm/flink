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

package org.apache.flink.table.ml.lib.splitarr;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 *
 */
public class SplitArrayFunction extends TableFunction<Row> {
	private final RowTypeInfo resultTypeInfo;
	private final int[] outputDims;
	private final int totalDim;
	private final boolean[] resultArrayFlags;

	public SplitArrayFunction(RowTypeInfo resultTypeInfo, int[] outputDims, boolean[] resultArrayFlags) {
		this.resultTypeInfo = resultTypeInfo;
		this.outputDims = outputDims;
		this.totalDim = Arrays.stream(outputDims).reduce(0, Integer::sum);
		this.resultArrayFlags = resultArrayFlags;
	}

	public void eval(double[] input) {
		assert input.length == totalDim;
		Row row = new Row(outputDims.length);
		int pointer = 0;
		for (int i = 0; i < outputDims.length; i++) {
			if (resultArrayFlags[i]) {
				int l = outputDims[i];
				double[] result = new double[l];
				System.arraycopy(input, pointer, result, 0, l);
				row.setField(i, result);
			} else {
				row.setField(i, input[pointer]);
			}
			pointer += outputDims[i];
		}
		collect(row);
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return resultTypeInfo;
	}
}
