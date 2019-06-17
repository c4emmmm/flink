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
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.common.ModelFormat;
import org.apache.flink.ml.api.misc.exportation.ExporterLoader;
import org.apache.flink.ml.api.misc.importation.ImporterLoader;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.ml.lib.kmeans.KMeans;
import org.apache.flink.table.ml.lib.kmeans.KMeansModel;
import org.apache.flink.table.ml.lib.linear.regression.LinearRegression;
import org.apache.flink.table.ml.lib.linear.regression.LinearRegressionModel;
import org.apache.flink.table.ml.lib.pmml.evaluate.PMMLEvaluateTransformer;
import org.apache.flink.table.ml.lib.pmml.load.PMMLImporter;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;
import org.apache.flink.table.ml.lib.splitarr.SplitArray;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PMMLTest {

	public static Table servePMMLModel(
		TableEnvironment tEnv,
		String pmmlFile,
		Table dataTable) throws
		Exception {
		String restoredPMML = PMMLUtil.readFile(pmmlFile);
		Model pmmlServe = ImporterLoader.getImporter(ModelFormat.PMML).load(restoredPMML);
		return pmmlServe.transform(tEnv, dataTable);
	}

	@Test
	public void testLinearRegression() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		//get data table for both train and predict in this case
		Table dataTable = getLinearRegresssionDataTable(bEnv, tEnv);

		//train lr model
		LinearRegressionModel lrModel = trainLinearRegressionModel(tEnv, dataTable);

		//define pmml file path
		String file = "/Users/hidden/Temp/linearRegression.pmml";

		//export and save pmml
		String lrPMML =
			(String) ExporterLoader.getExporter(ModelFormat.PMML, lrModel).export(lrModel);
		PMMLUtil.writeFile(lrPMML, file);

		//adapt data to pmml input, which do not accept array as input
		SplitArray splitArray =
			new SplitArray().setResultDim(new int[]{1, 1, 1}).setInputCol("feat")
				.setOutputCols(new String[]{"feat_0", "feat_1", "feat_2"});
		Table splitTable = splitArray.transform(tEnv, dataTable);

		//serve with pmml file
		Table pmmlResult = servePMMLModel(tEnv, file, splitTable);

		//print result
		for (Row r : tEnv.toDataSet(pmmlResult, Row.class).collect()) {
			System.out.println(rowToString(r));
		}
		System.out.println("---------------------------");

		//pmmlServe can also save as json and restored
		//create pmmlServe
		PMMLEvaluateTransformer pmmlServe = new PMMLImporter().load(PMMLUtil.readFile(file));

		//pmmlServe to json
		String pmmlServeJson = pmmlServe.toJson();
		System.out.println(pmmlServeJson);

		//load pmmlServe from json
		PMMLEvaluateTransformer jsonRestoredPmmlServe = new PMMLEvaluateTransformer();
		jsonRestoredPmmlServe.loadJson(pmmlServeJson);

		//serve with pmmlServe restored from json
		Table jsonResult = jsonRestoredPmmlServe.transform(tEnv, splitTable);

		//print result
		for (Row r : tEnv.toDataSet(jsonResult, Row.class).collect()) {
			System.out.println(rowToString(r));
		}
	}

	@Test
	public void testKmeans() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		Table dataTable = getKMeansDataTable(bEnv, tEnv);
		KMeansModel kmeansModel = trainKMeansModel(tEnv, dataTable);

		String file = "/Users/hidden/Temp/kmeans.pmml";

		String kmeansPMML =
			(String) ExporterLoader.getExporter(ModelFormat.PMML, kmeansModel).export(kmeansModel);
		PMMLUtil.writeFile(kmeansPMML, file);

		SplitArray splitArray =
			new SplitArray().setResultDim(new int[]{1, 1}).setInputCol("point")
				.setOutputCols(new String[]{"point_0", "point_1"});
		Table splitTable = splitArray.transform(tEnv, dataTable);

		Table pmmlResult = servePMMLModel(tEnv, file, splitTable);

		for (Row r : tEnv.toDataSet(pmmlResult, Row.class).collect()) {
			System.out.println(rowToString(r));
		}
	}

	public static String rowToString(Row r) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		Object[] values = new Object[r.getArity()];
		for (int i = 0; i < values.length; i++) {
			values[i] = r.getField(i);
		}
		return mapper.writeValueAsString(values);
	}

	private Table getKMeansDataTable(ExecutionEnvironment bEnv, BatchTableEnvironment tEnv) {
		List<Row> data = createKMeansData();
		DataSet<Row> dataSet = bEnv.fromCollection(data);
		return tEnv.fromDataSet(dataSet).select("f0 as point, f1 as label");
	}

	private KMeansModel trainKMeansModel(BatchTableEnvironment tEnv, Table dataTable) throws
		Exception {
		KMeans kmeans = new KMeans();
		kmeans.setK(3).setTolerance(0.001).setMaxIter(100)
			.setInputCol("point").setPredictionCol("pred");
		KMeansModel kmeansModel = kmeans.fit(tEnv, dataTable);

		Table predTable = kmeansModel.transform(tEnv, dataTable).select("point, label, pred");

		for (Row r : tEnv.toDataSet(predTable, Row.class).collect()) {
			System.out.println(rowToString(r));
		}
		System.out.println("-------------------------------------------------");
		System.out.println();
		return kmeansModel;
	}

	private Table getLinearRegresssionDataTable(
		ExecutionEnvironment bEnv,
		BatchTableEnvironment tEnv) {
		List<Row> data = createLinearRegressionData();
		DataSet<Row> dataSet = bEnv.fromCollection(data);
		return tEnv.fromDataSet(dataSet).select("f0 as feat, f1 as label");
	}

	private LinearRegressionModel trainLinearRegressionModel(
		BatchTableEnvironment tEnv,
		Table dataTable) throws
		Exception {
		LinearRegression lr = new LinearRegression();
		lr.setDim(3).setInitLearningRate(1).setMaxIter(100).setTolerance(0.03)
			.setFeatureCol("feat").setLabelCol("label").setPredictionCol("pred");
		LinearRegressionModel lrModel = lr.fit(tEnv, dataTable);

		Table predTable = lrModel.transform(tEnv, dataTable).select("feat, label, pred");

		for (Row r : tEnv.toDataSet(predTable, Row.class).collect()) {
			System.out.println(rowToString(r));
		}
		System.out.println("-------------------------------------------------");
		System.out.println();
		return lrModel;
	}

	private List<Row> createKMeansData() {
		double[][] centroids =
			new double[][]{new double[]{1, 1}, new double[]{0.3, -0.7}, new double[]{-0.5, 0.2}};
		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			Row r = new Row(2);
			if (Math.random() > 0.75) {
				r.setField(0, randomMove(centroids[0]));
				r.setField(1, 0);
			} else if (Math.random() > 0.4) {
				r.setField(0, randomMove(centroids[1]));
				r.setField(1, 1);
			} else {
				r.setField(0, randomMove(centroids[2]));
				r.setField(1, 2);
			}
			data.add(r);
		}

		return data;
	}

	private List<Row> createLinearRegressionData() {
		ArrayRealVector w = new ArrayRealVector(new double[]{1, 2, 3, 4});
		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			Row r = new Row(2);
			double[] feat = new double[]{Math.random(), Math.random(),
				Math.random()};
			double[] featWithIntercept = new double[]{feat[0], feat[1], feat[2], 1};
			double label =
				w.dotProduct(new ArrayRealVector(featWithIntercept)) * (0.95 + Math.random() * 0.1);
			r.setField(0, feat);
			r.setField(1, label);
			data.add(r);
		}

		return data;
	}

	private double[] randomMove(double[] centroid) {
		return new double[]{
			centroid[0] + Math.random() * 0.4 - 0.2,
			centroid[1] + Math.random() * 0.4 - 0.2,
		};
	}
}
