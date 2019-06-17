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

import org.apache.flink.table.ml.lib.kmeans.KMeansModel;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;

import org.dmg.pmml.Array;
import org.dmg.pmml.CompareFunction;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.ComplexArray;
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
import org.dmg.pmml.SquaredEuclidean;
import org.dmg.pmml.clustering.Cluster;
import org.dmg.pmml.clustering.ClusteringField;
import org.dmg.pmml.clustering.ClusteringModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class KMeansPMMLAppender implements PMMLAppender<KMeansModel> {
	@Override
	public void append(KMeansModel model, PMML pmml) {
		double[][] clusterCenters = model.getCentroids();
		int dim = clusterCenters[0].length;

		List<ClusteringField> clusteringFields = new ArrayList<>();

		String inputCol = model.getInputCol();
		for (int i = 0; i < dim; i++) {
			String subInputCol = inputCol + "_" + i;
			FieldName subInputField = FieldName.create(subInputCol);
			PMMLUtil
				.registerDataFieldIfAbsent(pmml, subInputField, OpType.CONTINUOUS, DataType.DOUBLE);
			clusteringFields.add(new ClusteringField(subInputField));
		}

		FieldName predField = FieldName.create(model.getPredictionCol());
		PMMLUtil.registerDataFieldIfAbsent(pmml, predField, OpType.CONTINUOUS, DataType.INTEGER);

		MiningSchema miningSchema = new MiningSchema();
		PMMLUtil.registerMiningSchemaInput(pmml, miningSchema, new String[]{predField.getValue()});
		miningSchema.addMiningFields(
			new MiningField(predField).setUsageType(MiningField.UsageType.TARGET));

		Output modelOutput = new Output();
		modelOutput.addOutputFields(
			new OutputField(predField, DataType.INTEGER).setOpType(OpType.CONTINUOUS)
				.setResultFeature(ResultFeature.PREDICTED_VALUE));

		List<Cluster> clusters = new ArrayList<>();
		for (int i = 0; i < clusterCenters.length; i++) {
			List<Double> c = Arrays.stream(clusterCenters[i]).boxed().collect(Collectors.toList());
			Cluster cluster = new Cluster().setId(String.valueOf(i))
				.setArray(new ComplexArray().setType(Array.Type.REAL).setValue(c));
			clusters.add(cluster);
		}

		ComparisonMeasure comparisonMeasure = new ComparisonMeasure(ComparisonMeasure.Kind.DISTANCE)
			.setCompareFunction(CompareFunction.ABS_DIFF)
			.setMeasure(new SquaredEuclidean());

		ClusteringModel clusteringModel = new ClusteringModel(MiningFunction.CLUSTERING,
			ClusteringModel.ModelClass.CENTER_BASED,
			clusters.size(), miningSchema, comparisonMeasure,
			clusteringFields, clusters);
		clusteringModel.setOutput(modelOutput);

		pmml.addModels(clusteringModel);
	}
}
