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

package org.apache.flink.table.ml.lib.pmml.appender;

import org.apache.flink.table.ml.lib.connect.Connect;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;

import org.dmg.pmml.DataType;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ConnectAppender implements PMMLAppender<Connect> {
	@Override
	public void append(Connect connect, PMML pmml) {
		int totalDim = connect.getDim();
		Set<String> inputArrayCols = new HashSet<>(Arrays.asList(connect.getArrayCols()));

		String[] inputCols = connect.getInputCols();
		int[] inputDim = connect.getInputDim();
		FieldName[] inputFields = new FieldName[totalDim];
		int idx = 0;
		for (int i = 0; i < inputCols.length; i++) {
			if (inputDim[i] == 1 && !inputArrayCols.contains(inputCols[i])) {
				String subInputCol = inputCols[i];
				inputFields[idx] = FieldName.create(subInputCol);
				idx++;
			} else {
				for (int j = 0; j < inputDim[i]; j++) {
					String subInputCol = inputCols[i] + "_" + j;
					inputFields[idx] = FieldName.create(subInputCol);
					idx++;
				}
			}
		}
		for (FieldName inputField : inputFields) {
			PMMLUtil
				.registerDataFieldIfAbsent(pmml, inputField, OpType.CONTINUOUS, DataType.DOUBLE);
		}

		String outputCol = connect.getOutputCol();
		DerivedField[] derivedFields = new DerivedField[totalDim];
		for (int i = 0; i < totalDim; i++) {
			String subOutputCol = outputCol + "_" + i;
			DerivedField derivedField = new DerivedField();
			derivedField.setName(FieldName.create(subOutputCol));
			derivedField.setExpression(new FieldRef(inputFields[i]));
			derivedFields[i] = derivedField;
		}

		PMMLUtil.getTransformationDictionary(pmml).addDerivedFields(derivedFields);
	}
}
