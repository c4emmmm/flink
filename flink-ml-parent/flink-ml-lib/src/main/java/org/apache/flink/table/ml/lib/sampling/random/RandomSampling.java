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

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;

/**
 *
 */
public class RandomSampling implements Transformer<RandomSampling> {
	public static final ParamInfo<Integer> COUNT = ParamInfoFactory
		.createParamInfo("count", Integer.class)
		.setDescription("output count")
		.setRequired().build();

	private final Params params = new Params();

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		int count = getCount();
		String[] inputFields = input.getSchema().getFieldNames();

		RandomMarker rand = new RandomMarker(count);
		String funcUniqName = "rand_marker_" + System.currentTimeMillis();
		tEnv.registerFunction(funcUniqName, rand);

		String inExpr = ExpressionUtil.genSelectAllExpr(inputFields);
		String randExpr = ExpressionUtil
			.genAppendUdfCallExpr(funcUniqName, "", "sample_flag", inputFields);

		return input.select(randExpr)
			.where("sample_flag > 0")
			.orderBy("sample_flag.desc")
			.fetch(count)
			.select(inExpr);
	}

	@Override
	public Params getParams() {
		return params;
	}

	public RandomSampling setCount(int count) {
		return set(COUNT, count);
	}

	public int getCount() {
		return get(COUNT);
	}
}
