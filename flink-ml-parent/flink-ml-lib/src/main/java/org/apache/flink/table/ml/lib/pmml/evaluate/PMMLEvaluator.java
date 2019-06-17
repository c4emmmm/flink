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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;
import org.apache.flink.types.Row;

import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PMMLEvaluator extends TableFunction<Row> {
	private final String pmml;
	private final RowTypeInfo outputRowTypeInfo;
	private final int outputArity;
	private final String[] outputRawFieldNames;

	private Evaluator evaluator;
	private List<InputField> inputFields;

	PMMLEvaluator(String pmml, RowTypeInfo outputRowTypeInfo, String[] outputRawFieldNames) {
		this.pmml = pmml;
		this.outputRowTypeInfo = outputRowTypeInfo;
		this.outputArity = outputRowTypeInfo.getFieldNames().length;
		this.outputRawFieldNames = outputRawFieldNames;
	}

	public void eval(Object... input) {
		if (evaluator == null) {
			evaluator = PMMLUtil.createEvaluator(PMMLUtil.parsePMMLString(pmml));
			inputFields = evaluator.getInputFields();
		}

		assert input.length == inputFields.size();
		Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
		for (int i = 0; i < inputFields.size(); i++) {
			InputField inputField = inputFields.get(i);
			FieldName inputFieldName = inputField.getName();
			Object rawValue = input[i];
			FieldValue inputFieldValue = inputField.prepare(rawValue);
			arguments.put(inputFieldName, inputFieldValue);
		}

		Map<FieldName, ?> results = evaluator.evaluate(arguments);
		Map<String, Object> simpleResultMap = new HashMap<>();
		for (Map.Entry<FieldName, ?> r : results.entrySet()) {
			if (r.getKey() == null) {
				continue;
			}
			simpleResultMap.put(r.getKey().getValue(), r.getValue());
		}

		Row row = new Row(outputArity);
		for (int i = 0; i < outputArity; i++) {
			row.setField(i, simpleResultMap.get(outputRawFieldNames[i]));
		}
		collect(row);
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return outputRowTypeInfo;
	}
}
