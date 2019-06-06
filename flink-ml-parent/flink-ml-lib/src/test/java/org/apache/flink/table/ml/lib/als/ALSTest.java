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

package org.apache.flink.table.ml.lib.als;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ALSTest {
	@Test
	public void testALS() throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.createLocalEnvironment(2);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(bEnv, new TableConfig());

		List<Row> allData = createData();
		DataSet<Row> allDataSet = bEnv.fromCollection(allData);
		Table allDataTable = tEnv.fromDataSet(allDataSet).select("f0 as user, f1 as item, f2 as " +
			"rating");

		List<Row> trainData = sampling(allData, 0.6);
		DataSet<Row> trainSet = bEnv.fromCollection(trainData);
		Table trainTable = tEnv.fromDataSet(trainSet).select("f0 as user, f1 as item, f2 as " +
			"rating");

		ALS als = new ALS();
		als.setRank(2).setLambda(1).setMaxIter(10).setTolerance(0.1)
			.setUserCol("user").setItemCol("item").setLabelCol("rating").setPredictionCol("pred")
			.setModelFilePath("./");
		ALSModel alsModel = als.fit(tEnv, trainTable);

		Table predTable = alsModel.transform(tEnv, allDataTable).select("user, item, rating, pred");
		for (Row r : tEnv.toDataSet(predTable, Row.class).collect()) {
			System.out.println(
				"user=" + r.getField(0) + ", item=" + r.getField(1) +
					", rating=" + r.getField(2) + ", pred=" + r.getField(3));
		}
	}

	private List<Row> sampling(List<Row> allData, double rate) {
		List<Row> res = new ArrayList<>();
		for (Row r : allData) {
			if (Math.random() < rate) {
				res.add(r);
			}
		}
		return res;
	}

	private List<Row> createData() {
		double[][] userFactors = new double[10][2];
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 2; j++) {
				userFactors[i][j] = Math.floor(Math.random() * 10 + 1);
			}
		}
		double[][] itemFactors = new double[2][20];
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < 20; j++) {
				itemFactors[i][j] = Math.floor(Math.random() * 10 + 1);
			}
		}

		double[][] rating = new Array2DRowRealMatrix(userFactors)
			.multiply(new Array2DRowRealMatrix(itemFactors)).getData();

		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 20; j++) {
				Row r = new Row(3);
				r.setField(0, (long) i);
				r.setField(1, (long) j);
				r.setField(2, rating[i][j]);
				data.add(r);
			}
		}

		return data;
	}
}
