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

package org.apache.flink.table.ml.lib.logistic.regression;

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.ml.lib.common.params.column.WithFeatureCol;
import org.apache.flink.table.ml.lib.common.params.column.WithLabelCol;
import org.apache.flink.table.ml.lib.common.params.column.WithPredictionCol;
import org.apache.flink.table.ml.lib.gradient.descent.Gradient;
import org.apache.flink.table.ml.lib.gradient.descent.GradientDescent;
import org.apache.flink.table.ml.lib.gradient.descent.WithGradientDescentParams;

/**
 *
 */
public class LogisticRegression implements Estimator<LogisticRegression, LogisticRegressionModel>,
	WithGradientDescentParams<LogisticRegression>,
	WithFeatureCol<LogisticRegression>,
	WithLabelCol<LogisticRegression>,
	WithPredictionCol<LogisticRegression> {
	private final GradientDescent estimator =
		new GradientDescent().setGradientType(Gradient.Type.LOGISTIC);

	public LogisticRegression() {
	}

	@Override
	public LogisticRegressionModel fit(TableEnvironment tEnv, Table input) {
		return new LogisticRegressionModel(estimator.fit(tEnv, input));
	}

	@Override
	public Params getParams() {
		return estimator.getParams();
	}

	@Override
	public String toJson() {
		return estimator.toJson();
	}

	@Override
	public void loadJson(String json) {
		estimator.loadJson(json);
	}
}
