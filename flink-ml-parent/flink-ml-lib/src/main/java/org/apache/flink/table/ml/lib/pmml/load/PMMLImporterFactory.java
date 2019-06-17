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

package org.apache.flink.table.ml.lib.pmml.load;

import org.apache.flink.ml.api.misc.common.ModelFormat;
import org.apache.flink.ml.api.misc.importation.Importer;
import org.apache.flink.ml.api.misc.importation.ImporterFactory;
import org.apache.flink.table.ml.lib.pmml.evaluate.PMMLEvaluateTransformer;

import java.util.Map;

/**
 *
 */
public class PMMLImporterFactory extends ImporterFactory {

	@Override
	public Importer<PMMLEvaluateTransformer, String> create(Map<String, String> properties) {
		return new PMMLImporter();
	}

	@Override
	public ModelFormat getSourceFormat() {
		return ModelFormat.PMML;
	}
}
