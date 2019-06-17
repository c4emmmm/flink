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
import org.apache.flink.table.ml.lib.scale.Scale;

import org.dmg.pmml.Apply;
import org.dmg.pmml.Constant;
import org.dmg.pmml.DataType;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;

/**
 *
 */
public class ScalePMMLAppender implements PMMLAppender<Scale> {
	@Override
	public void append(Scale scale, PMML pmml) {
		FieldName inputField = FieldName.create(scale.getInputCol());
		FieldName outputField = FieldName.create(scale.getOutputCol());

		PMMLUtil.registerDataFieldIfAbsent(pmml, inputField, OpType.CONTINUOUS, DataType.DOUBLE);

		Constant multiplier = new Constant((scale.getOutputMax() - scale.getOutputMin()) /
			(scale.getInputMax() - scale.getInputMin()));
		Constant inputMin = new Constant(scale.getInputMin());
		Constant outputMin = new Constant(scale.getOutputMin());
		Apply inputDelta = new Apply("-").addExpressions(new FieldRef(inputField), inputMin);
		Apply outputDelta = new Apply("*").addExpressions(inputDelta, multiplier);
		Apply outputValue = new Apply("+").addExpressions(outputMin, outputDelta);

		DerivedField derivedField = new DerivedField(OpType.CONTINUOUS, DataType.DOUBLE);
		derivedField.setName(outputField);
		derivedField.setExpression(outputValue);

		PMMLUtil.getTransformationDictionary(pmml).addDerivedFields(derivedField);
	}
}
