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

package org.apache.flink.table.ml.lib.connect;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.ml.lib.common.params.column.WithInputCols;
import org.apache.flink.table.ml.lib.common.params.column.WithOutputCol;

/**
 * @param <T>
 */
public interface WithConnectParams<T extends WithConnectParams<T>> extends WithParams<T>,
	WithInputCols<T>, WithOutputCol<T> {
	ParamInfo<int[]> INPUT_DIM = ParamInfoFactory
		.createParamInfo("input_dim", int[].class)
		.setDescription("input_dim")
		.setRequired().build();
	ParamInfo<String[]> ARRAY_COLS = ParamInfoFactory
		.createParamInfo("double_cols", String[].class)
		.setDescription("columns whose value is double array, other fields must be double")
		.setRequired()
		.setHasDefaultValue(new String[0]).build();
	ParamInfo<Integer> DIM = ParamInfoFactory
		.createParamInfo("dim", Integer.class)
		.setDescription("dim")
		.setRequired().build();

	default int[] getInputDim() {
		return get(INPUT_DIM);
	}

	default T setInputDim(int[] inputDim) {
		return set(INPUT_DIM, inputDim);
	}

	default String[] getArrayCols() {
		return get(ARRAY_COLS);
	}

	default T setArrayCols(String[] arrayCols) {
		return set(ARRAY_COLS, arrayCols);
	}

	default int getDim() {
		return get(DIM);
	}

	default T setDim(int dim) {
		return set(DIM, dim);
	}
}
