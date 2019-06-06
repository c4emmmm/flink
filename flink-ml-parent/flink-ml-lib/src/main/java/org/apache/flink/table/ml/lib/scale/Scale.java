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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;
import org.apache.flink.table.ml.lib.util.TableUtil;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.table.ml.lib.util.TableUtil.toDataSet;

/**
 * Scale a numerical column into the range [OutputMin, OutputMax].
 */
public class Scale implements Transformer<Scale>, WithScaleParams<Scale> {
	private Params params = new Params();

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		String[] inputFields = input.getSchema().getFieldNames();
		String inputCol = getInputCol();
		String outputCol = getOutputCol();

		Double inMin = getInputMin();
		Double inMax = getInputMax();
		if (inMin == null || inMax == null) {
			TableUtil.assertBatchTableEnvironment(tEnv);
			Tuple2<Double, Double> minMax = extractBatchMinMax(tEnv, input);
			inMin = minMax.f0;
			inMax = minMax.f1;
		}

		ScaleFunction func = new ScaleFunction(inMin, inMax, getOutputMin(), getOutputMax());
		String funcUniqName = "scale_" + System.currentTimeMillis();
		tEnv.registerFunction(funcUniqName, func);

		String expr =
			ExpressionUtil.genAppendUdfCallExpr(funcUniqName, inputCol, outputCol, inputFields);
		return input.select(expr);
	}

	private Tuple2<Double, Double> extractBatchMinMax(TableEnvironment tEnv, Table input) {
		String inputCol = getInputCol();
		Table resultTable = input.select(inputCol + ".min as inMin, " + inputCol + ".max as inMax");
		DataSet<Row> resultSet = toDataSet(tEnv, resultTable);

		List<Row> resultRows;
		try {
			resultRows = resultSet.collect();
			assert resultRows.size() == 1;
		} catch (Exception e) {
			throw new RuntimeException("Failed to acquire min and max of input.", e);
		}

		Row inputStat = resultRows.get(0);
		double inMin = (double) inputStat.getField(0);
		double inMax = (double) inputStat.getField(1);
		return new Tuple2<>(inMin, inMax);
	}

	@Override
	public Params getParams() {
		return params;
	}
}
