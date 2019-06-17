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
import org.apache.flink.ml.api.core.PipelineStage;
import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.common.ModelFormat;
import org.apache.flink.ml.api.misc.exportation.Exporter;
import org.apache.flink.table.ml.lib.kmeans.KMeansModel;
import org.apache.flink.table.ml.lib.linear.regression.LinearRegressionModel;
import org.apache.flink.table.ml.lib.pmml.appender.PMMLAppenderFactory;
import org.apache.flink.table.ml.lib.pmml.util.PMMLUtil;

import org.dmg.pmml.PMML;

import java.util.Arrays;
import java.util.List;

/**
 * Example of PMML exporter using {@link org.apache.flink.table.ml.lib.pmml.appender.PMMLAppender}.
 */
public class PMMLExporter extends Exporter<Model<?>, String> {
	@Override
	@SuppressWarnings("unchecked")
	public String export(Model<?> model) {
		Pipeline pipeline;
		if (model instanceof Pipeline) {
			pipeline = (Pipeline) model;
		} else {
			pipeline = new Pipeline().appendStage(model);
		}

		PMML pmml = new PMML();
		for (PipelineStage stage : pipeline.getStages()) {
			assert (stage instanceof Transformer);
			Transformer transformer = (Transformer) stage;
			PMMLAppenderFactory.getAppender(transformer).append(transformer, pmml);
		}
		return PMMLUtil.toPMMLString(pmml);
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
