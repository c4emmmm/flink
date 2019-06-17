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

import org.apache.flink.table.ml.lib.linear.regression.LinearRegressionModel;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;

import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunction;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.Output;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.dmg.pmml.ResultFeature;
import org.dmg.pmml.regression.NumericPredictor;
import org.dmg.pmml.regression.RegressionModel;
import org.dmg.pmml.regression.RegressionTable;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class LinearRegressionPMMLAppender implements PMMLAppender<LinearRegressionModel> {
	@Override
	public void append(LinearRegressionModel model, PMML pmml) {
		double[] fullWeights = model.getWeights();
		double[] weights = new double[fullWeights.length - 1];
		System.arraycopy(fullWeights, 0, weights, 0, weights.length);
		double interceptor = fullWeights[fullWeights.length - 1];
		int dim = weights.length;

		RegressionTable regression = new RegressionTable();
		regression.setIntercept(interceptor);

		String inputCol = model.getFeatureCol();
		for (int i = 0; i < dim; i++) {
			String subInputCol = inputCol + "_" + i;
			FieldName subInputField = FieldName.create(subInputCol);
			PMMLUtil
				.registerDataFieldIfAbsent(pmml, subInputField, OpType.CONTINUOUS, DataType.DOUBLE);
			regression.addNumericPredictors(new NumericPredictor(subInputField, weights[i]));
		}

		FieldName predField = FieldName.create(model.getPredictionCol());
		PMMLUtil.registerDataFieldIfAbsent(pmml, predField, OpType.CONTINUOUS, DataType.DOUBLE);

		MiningSchema miningSchema = new MiningSchema();
		PMMLUtil.registerMiningSchemaInput(pmml, miningSchema, new String[]{predField.getValue()});
		miningSchema.addMiningFields(
			new MiningField(predField).setUsageType(MiningField.UsageType.TARGET));

		Output modelOutput = new Output();
		modelOutput.addOutputFields(
			new OutputField(predField, DataType.DOUBLE).setOpType(OpType.CONTINUOUS)
				.setResultFeature(ResultFeature.PREDICTED_VALUE));

		List<RegressionTable> regressionTables = new ArrayList<>();
		regressionTables.add(regression);

		RegressionModel regressionModel = new RegressionModel(MiningFunction.REGRESSION,
			miningSchema, regressionTables);
		regressionModel.setOutput(modelOutput);

		pmml.addModels(regressionModel);
	}
}
