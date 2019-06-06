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

package org.apache.flink.table.ml.lib.scale;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.ml.lib.common.params.column.WithInputCol;
import org.apache.flink.table.ml.lib.common.params.column.WithOutputCol;

/**
 * @param <T>
 */
public interface WithScaleParams<T extends WithScaleParams<T>> extends WithParams<T>,
	WithInputCol<T>, WithOutputCol<T> {
	ParamInfo<Double> INPUT_MAX = ParamInfoFactory
		.createParamInfo("input_max", Double.class).setDescription(
			"the maximum of input, can learn from batch data, must set manually for streaming data")
		.setOptional().build();
	ParamInfo<Double> INPUT_MIN = ParamInfoFactory
		.createParamInfo("input_min", Double.class).setDescription(
			"the minimum of input, can learn from batch data, must set manually for streaming data")
		.setOptional().build();
	ParamInfo<Double> OUTPUT_MAX = ParamInfoFactory
		.createParamInfo("output_max", Double.class)
		.setDescription("the maximum of the range of output values, 1 by default")
		.setHasDefaultValue(1.0).build();
	ParamInfo<Double> OUTPUT_MIN = ParamInfoFactory
		.createParamInfo("output_min", Double.class)
		.setDescription("the minimum of the range of output values, 0 by defaultt")
		.setHasDefaultValue(0.0).build();

	default Double getInputMax() {
		return get(INPUT_MAX);
	}

	default T setInputMax(double inputMax) {
		return set(INPUT_MAX, inputMax);
	}

	default Double getInputMin() {
		return get(INPUT_MIN);
	}

	default T setInputMin(double inputMin) {
		return set(INPUT_MIN, inputMin);
	}

	default double getOutputMax() {
		return get(OUTPUT_MAX);
	}

	default T setOutputMax(double outputMax) {
		return set(OUTPUT_MAX, outputMax);
	}

	default double getOutputMin() {
		return get(OUTPUT_MIN);
	}

	default T setOutputMin(double outputMin) {
		return set(OUTPUT_MIN, outputMin);
	}
}
