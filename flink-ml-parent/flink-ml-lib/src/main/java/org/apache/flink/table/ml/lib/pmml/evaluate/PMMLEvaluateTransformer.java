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

package org.apache.flink.table.ml.lib.pmml.evaluate;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;
import org.apache.flink.table.ml.lib.util.TableUtil;

import org.dmg.pmml.DataType;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.OutputField;

import java.util.List;

/**
 *
 */
public class PMMLEvaluateTransformer implements Model<PMMLEvaluateTransformer> {
	private static final ParamInfo<String> PMML_DEFINE =
		ParamInfoFactory.createParamInfo("pmml", String.class)
			.setDescription("string format pmml")
			.setRequired().build();

	private final Params params = new Params();

	public PMMLEvaluateTransformer() {
	}

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		PMML pmml = PMMLUtil.parsePMMLString(getPMML());
		Evaluator pmmlEvaluator = PMMLUtil.createEvaluator(pmml);
		List<InputField> inputFields = pmmlEvaluator.getInputFields();
		String[] inputFieldNames =
			inputFields.stream().map(f -> f.getName().getValue()).toArray(String[]::new);

		List<OutputField> outputFields = pmmlEvaluator.getOutputFields();
		String[] outputRawFieldName = outputFields.stream()
			.map(f -> f.getName().getValue())
			.toArray(String[]::new);
		String[] outputFieldNames =
			outputFields.stream()
				.map(f -> f.getName().getValue().replace("(", "_").replace(")", "_"))
				.toArray(String[]::new);
		TypeInformation<?>[] outputTypeInfos =
			outputFields.stream().map(f -> toFlinkType(f.getDataType()))
				.toArray(TypeInformation<?>[]::new);
		RowTypeInfo outputRowTypeInfo = new RowTypeInfo(outputTypeInfos, outputFieldNames);

		PMMLEvaluator evaluator =
			new PMMLEvaluator(getPMML(), outputRowTypeInfo, outputRawFieldName);
		String evaluatorName = "evaluator_" + System.nanoTime();
		TableUtil.registerTableFunction(tEnv, evaluatorName, evaluator);

		StringBuilder outputAliases = new StringBuilder().append("(");
		for (int i = 0; i < outputFieldNames.length; i++) {
			if (i != 0) {
				outputAliases.append(",");
			}
			outputAliases.append(outputFieldNames[i]);
		}
		outputAliases.append(")");

		String callExpr = ExpressionUtil.genUdfCallExpr(evaluatorName, inputFieldNames,
			outputAliases.toString());
		return input.joinLateral(callExpr);
	}

	@Override
	public Params getParams() {
		return params;
	}

	public PMMLEvaluateTransformer setPMML(String pmml) {
		return set(PMML_DEFINE, pmml);
	}

	private String getPMML() {
		return get(PMML_DEFINE);
	}

	private TypeInformation<?> toFlinkType(DataType pmmlDataType) {
		switch (pmmlDataType) {
			case STRING:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case INTEGER:
				return BasicTypeInfo.INT_TYPE_INFO;
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			default:
				throw new RuntimeException("Unsupported pmml type:" + pmmlDataType);
		}
	}
}
