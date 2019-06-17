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

package org.apache.flink.table.ml.lib.pmml;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.api.misc.common.ModelFormat;
import org.apache.flink.ml.api.misc.exportation.ExporterLoader;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.ml.lib.connect.Connect;
import org.apache.flink.table.ml.lib.linear.regression.LinearRegression;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;
import org.apache.flink.table.ml.lib.scale.Scale;
import org.apache.flink.types.Row;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PipelinePMMLTest {

	@Test
	public void testPipeline() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		Table dataTable = getTestDataTable(bEnv, tEnv);
		Pipeline pipelineModel = trainPipelineModel(tEnv, dataTable);

		String file = "/Users/hidden/Temp/pipeline.pmml";

		String pipelinePMML =
			(String) ExporterLoader.getExporter(ModelFormat.PMML, pipelineModel).export(pipelineModel);
		PMMLUtil.writeFile(pipelinePMML, file);

		Table splitTable = dataTable;

		Table pmmlResult = PMMLTest.servePMMLModel(tEnv, file, splitTable);

		for (Row r : tEnv.toDataSet(pmmlResult, Row.class).collect()) {
			System.out.println(PMMLTest.rowToString(r));
		}
	}

	private Pipeline trainPipelineModel(BatchTableEnvironment tEnv, Table dataTable) throws
		Exception {
		Scale scaleX = new Scale();
		scaleX.setInputCol("x").setOutputCol("sx").setOutputMin(0).setOutputMax(1);
		Scale scaleY = new Scale();
		scaleY.setInputCol("y").setOutputCol("sy").setOutputMin(0).setOutputMax(1);
		Scale scaleZ = new Scale();
		scaleZ.setInputCol("z").setOutputCol("sz").setOutputMin(0).setOutputMax(1);

		Connect connect = new Connect();
		connect.setDim(3).setOutputCol("feat")
			.setInputCols(new String[]{"sx", "sy", "sz"})
			.setInputDim(new int[]{1, 1, 1});

		LinearRegression lr = new LinearRegression();
		lr.setDim(3).setInitLearningRate(1).setMaxIter(100).setTolerance(0.03)
			.setFeatureCol("feat").setLabelCol("label").setPredictionCol("pred");

		Pipeline pipeline = new Pipeline();
		pipeline.appendStage(scaleX).appendStage(scaleY).appendStage(scaleZ).appendStage(connect)
			.appendStage(lr);
		Pipeline pipelineModel = pipeline.fit(tEnv, dataTable);

		Table predTable = pipelineModel.transform(tEnv, dataTable).select("x, y, z, label, pred");

		for (Row r : tEnv.toDataSet(predTable, Row.class).collect()) {
			System.out.println(PMMLTest.rowToString(r));
		}
		System.out.println("-------------------------------------------------");
		System.out.println();

		return pipelineModel;
	}

	private Table getTestDataTable(ExecutionEnvironment bEnv, BatchTableEnvironment tEnv) {
		List<Row> data = createLinearRegressionData();
		DataSet<Row> dataSet = bEnv.fromCollection(data);
		return tEnv.fromDataSet(dataSet).select("f0 as x, f1 as y, f2 as z, f3 as label");
	}

	private List<Row> createLinearRegressionData() {
		ArrayRealVector w = new ArrayRealVector(new double[]{1, 2, 3, 4});
		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			Row r = new Row(4);
			double[] feat = new double[]{Math.random() * 5, Math.random() * 3,
				Math.random() * 2};
			double[] featWithIntercept = new double[]{feat[0], feat[1], feat[2], 1};
			double label =
				w.dotProduct(new ArrayRealVector(featWithIntercept)) * (0.95 + Math.random() * 0.1);
			r.setField(0, feat[0]);
			r.setField(1, feat[1]);
			r.setField(2, feat[2]);
			r.setField(3, label);
			data.add(r);
		}

		return data;
	}
}
