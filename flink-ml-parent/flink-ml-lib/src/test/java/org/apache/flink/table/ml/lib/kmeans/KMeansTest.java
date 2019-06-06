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

package org.apache.flink.table.ml.lib.kmeans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class KMeansTest {

	@Test
	public void testKMeans() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		List<Row> data = createData();
		DataSet<Row> dataSet = bEnv.fromCollection(data);
		Table dataTable = tEnv.fromDataSet(dataSet).select("f0 as point, f1 as label");

		KMeans kmeans = new KMeans();
		kmeans.setK(3).setTolerance(0.001).setMaxIter(100)
			.setInputCol("point").setPredictionCol("pred");
		KMeansModel kmeansModel = kmeans.fit(tEnv, dataTable);

		Table predTable = kmeansModel.transform(tEnv, dataTable).select("point, label, pred");
		for (Row r : tEnv.toDataSet(predTable, Row.class).collect()) {
			System.out.println(
				"point=" + Arrays.toString((double[]) r.getField(0)) +
					", label=" + r.getField(1) + ", pred=" + r.getField(2));
		}
	}

	private List<Row> createData() {
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

	private double[] randomMove(double[] centroid) {
		return new double[]{
			centroid[0] + Math.random() * 0.4 - 0.2,
			centroid[1] + Math.random() * 0.4 - 0.2,
		};
	}
}
