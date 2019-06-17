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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;
import org.apache.flink.table.ml.lib.util.TableUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class SplitArray implements Transformer<SplitArray>, WithSplitArrayParams<SplitArray> {
	private final Params params = new Params();

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		String inputCol = getInputCol();
		int[] outputDims = getResultDim();
		String[] outputCols = getOutputCols();
		Set<String> outputArrayCols = new HashSet<>(Arrays.asList(getResultArrayCols()));

		TypeInformation<?>[] outputTypes = new TypeInformation<?>[outputCols.length];
		boolean[] resultArrayFlags = new boolean[outputDims.length];
		for (int i = 0; i < outputCols.length; i++) {
			resultArrayFlags[i] = outputDims[i] > 1 || outputArrayCols.contains(outputCols[i]);
			outputTypes[i] = resultArrayFlags[i] ?
				PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO :
				BasicTypeInfo.DOUBLE_TYPE_INFO;
		}
		RowTypeInfo resultTypeInfo = new RowTypeInfo(outputTypes, outputCols);

		SplitArrayFunction func =
			new SplitArrayFunction(resultTypeInfo, outputDims, resultArrayFlags);
		String funcUniqName = "splieArr_" + System.nanoTime();
		TableUtil.registerTableFunction(tEnv, funcUniqName, func);

		String callExpr = ExpressionUtil.genUdfCallExpr(funcUniqName, inputCol, outputCols);
		return input.joinLateral(callExpr);
	}

	@Override
	public Params getParams() {
		return params;
	}
}
