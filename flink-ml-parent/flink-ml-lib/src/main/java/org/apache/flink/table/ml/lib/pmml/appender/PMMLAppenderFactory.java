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

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.table.ml.lib.connect.Connect;
import org.apache.flink.table.ml.lib.kmeans.KMeansModel;
import org.apache.flink.table.ml.lib.linear.regression.LinearRegressionModel;
import org.apache.flink.table.ml.lib.scale.Scale;
import org.apache.flink.table.ml.lib.splitarr.SplitArray;

/**
 *
 */
public class PMMLAppenderFactory {
	@SuppressWarnings("unchecked")
	public static <T extends Transformer<T>> PMMLAppender<T> getAppender(T transformer) {
		if (transformer instanceof KMeansModel) {
			return (PMMLAppender<T>) new KMeansPMMLAppender();
		} else if (transformer instanceof LinearRegressionModel) {
			return (PMMLAppender<T>) new LinearRegressionPMMLAppender();
		} else if (transformer instanceof Scale) {
			return (PMMLAppender<T>) new ScalePMMLAppender();
		} else if (transformer instanceof Connect) {
			return (PMMLAppender<T>) new ConnectAppender();
		} else if (transformer instanceof SplitArray) {
			return (PMMLAppender<T>) new SplitArrayAppender();
		} else {
			throw new RuntimeException();
		}
	}
}
