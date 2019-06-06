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

package org.apache.flink.ml;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.ml.lib.kmeans.KMeans;
import org.apache.flink.table.ml.lib.kmeans.KMeansModel;
import org.apache.flink.table.ml.lib.util.TableUtil;
import org.apache.flink.types.Row;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SimpleExample {
	@Test
	public void test() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		DataSet<Row> trainSet = bEnv.fromCollection(getTestPoints());
		Table trainInput = tEnv.fromDataSet(trainSet);

		KMeans kmeans = new KMeans().setK(4).setInputCol("f0").setPredictionCol("cluster");
		KMeansModel model = kmeans.fit(tEnv, trainInput);

		DataSet<Row> dataSet = bEnv.fromCollection(getTestPoints());
		Table input = tEnv.fromDataSet(dataSet);
		TableUtil.toDataSet(tEnv, model.transform(tEnv, input)).collect()
			.forEach(System.out::println);

		System.out.println("kmeans=" + kmeans.toJson());
		System.out.println("kmeansModel=" + model.toJson());
	}

	private List<Row> getTestPoints() {
		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			int cluster = (int) (Math.random() * 4);
			ArrayRealVector point = new ArrayRealVector(new double[]{Math.random() + 1,
				Math.random() + 1});
			switch (cluster) {
				case 0:
					point = point.ebeMultiply(new ArrayRealVector(new double[]{1, 1}));
					break;
				case 1:
					point = point.ebeMultiply(new ArrayRealVector(new double[]{1, -1}));
					break;
				case 2:
					point = point.ebeMultiply(new ArrayRealVector(new double[]{-1, -1}));
					break;
				case 3:
					point = point.ebeMultiply(new ArrayRealVector(new double[]{-1, 1}));
					break;
			}
			double[] p = point.toArray();
			Row row = new Row(1);
			row.setField(0, p);
			data.add(row);
		}
		return data;
	}
}
