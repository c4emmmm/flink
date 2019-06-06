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

package org.apache.flink.table.ml.lib.linear.regression;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class LinearRegressionTest {

	@Test
	public void testLinearRegression() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		List<Row> data = createData();
		DataSet<Row> dataSet = bEnv.fromCollection(data);
		Table dataTable = tEnv.fromDataSet(dataSet).select("f0 as feat, f1 as label");

		LinearRegression lr = new LinearRegression();
		lr.setDim(3).setInitLearningRate(1).setMaxIter(1000).setTolerance(0.022)
			.setFeatureCol("feat").setLabelCol("label").setPredictionCol("pred");
		LinearRegressionModel lrModel = lr.fit(tEnv, dataTable);

		Table predTable = lrModel.transform(tEnv, dataTable).select("feat, label, pred");
		for (Row r : tEnv.toDataSet(predTable, Row.class).collect()) {
			System.out.println(
				"feat=" + Arrays.toString((double[]) r.getField(0)) +
					", label=" + r.getField(1) + ", pred=" + r.getField(2));
		}
	}

	private List<Row> createData() {
		ArrayRealVector w = new ArrayRealVector(new double[]{1, 2, 3, 4});
		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
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
}
