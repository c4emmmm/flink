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

import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;
import org.apache.flink.table.ml.lib.splitarr.SplitArray;

import org.dmg.pmml.DataType;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

/**
 *
 */
public class SplitArrayAppender implements PMMLAppender<SplitArray> {
	@Override
	public void append(SplitArray split, PMML pmml) {
		int[] outputDim = split.getResultDim();
		int totalDim = IntStream.of(outputDim).sum();

		String inputCol = split.getInputCol();
		FieldName[] inputFields = new FieldName[totalDim];
		for (int i = 0; i < totalDim; i++) {
			String subInputCol = inputCol + "_" + i;
			FieldName subInputField = FieldName.create(subInputCol);
			inputFields[i] = subInputField;
			PMMLUtil
				.registerDataFieldIfAbsent(pmml, subInputField, OpType.CONTINUOUS, DataType.DOUBLE);
		}

		Set<String> resultArrayCols = new HashSet<>(Arrays.asList(split.getResultArrayCols()));
		String[] outputCol = split.getOutputCols();
		DerivedField[] derivedFields = new DerivedField[totalDim];
		int idx = 0;
		for (int i = 0; i < outputCol.length; i++) {
			if (outputDim[i] == 1 && !resultArrayCols.contains(outputCol[i])) {
				String subOutputCol = outputCol[i];
				DerivedField derivedField = new DerivedField();
				derivedField.setName(FieldName.create(subOutputCol));
				derivedField.setExpression(new FieldRef(inputFields[idx]));
				derivedFields[idx] = derivedField;
				idx++;
			} else {
				for (int j = 0; j < outputDim[i]; j++) {
					String subOutputCol = outputCol[i] + "_" + j;
					DerivedField derivedField = new DerivedField();
					derivedField.setName(FieldName.create(subOutputCol));
					derivedField.setExpression(new FieldRef(inputFields[idx]));
					derivedFields[idx] = derivedField;
					idx++;
				}
			}
		}

		PMMLUtil.getTransformationDictionary(pmml).addDerivedFields(derivedFields);
	}
}
