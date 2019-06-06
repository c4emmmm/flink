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

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class Connect implements Transformer<Connect>, WithConnectParams<Connect> {
	private Params params = new Params();

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		String[] inputFields = input.getSchema().getFieldNames();
		String[] inputCols = getInputCols();
		String outputCol = getOutputCol();
		int dim = getDim();

		Set<String> arrayCols = new HashSet<>(Arrays.asList(getArrayCols()));
		boolean[] arrayFlag = new boolean[inputCols.length];
		for (int i = 0; i < inputCols.length; i++) {
			arrayFlag[i] = arrayCols.contains(inputCols[i]);
		}

		ConnectFunction func = new ConnectFunction(dim, arrayFlag);
		String funcUniqName = "connect_" + System.currentTimeMillis();
		tEnv.registerFunction(funcUniqName, func);

		String expr = ExpressionUtil
			.genAppendUdfCallExpr(funcUniqName, inputCols, outputCol, inputFields);
		return input.select(expr);
	}

	@Override
	public Params getParams() {
		return params;
	}
}
