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

package org.apache.flink.table.ml.lib.pmml.export;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.api.misc.common.ModelFormat;
import org.apache.flink.ml.api.misc.exportation.Exporter;
import org.apache.flink.ml.api.misc.exportation.ExporterFactory;
import org.apache.flink.table.ml.lib.kmeans.KMeansModel;
import org.apache.flink.table.ml.lib.linear.regression.LinearRegressionModel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Example of PMML exporter using {@link org.apache.flink.table.ml.lib.pmml.appender.PMMLAppender}.
 */
public class PMMLExporterFactory extends ExporterFactory {

	@Override
	public Exporter create(Model model, Map<String, String> properties) {
		return new PMMLExporter();
	}

	@Override
	public ModelFormat getTargetFormat() {
		return ModelFormat.PMML;
	}

	@Override
	public List<String> supportedModelClassNames() {
		return Arrays.asList(
			Pipeline.class.getName(),
			KMeansModel.class.getName(),
			LinearRegressionModel.class.getName());
	}
}
