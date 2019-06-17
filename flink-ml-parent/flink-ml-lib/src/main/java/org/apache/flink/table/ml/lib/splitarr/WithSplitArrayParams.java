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

package org.apache.flink.table.ml.lib.splitarr;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.ml.lib.common.params.column.WithInputCol;
import org.apache.flink.table.ml.lib.common.params.column.WithOutputCols;

/**
 *
 */
public interface WithSplitArrayParams<T extends WithSplitArrayParams<T>> extends WithParams<T>,
	WithInputCol<T>, WithOutputCols<T> {
	ParamInfo<int[]> RESULT_DIM = ParamInfoFactory
		.createParamInfo("result_dim", int[].class)
		.setDescription("result_dim")
		.setRequired().build();
	ParamInfo<String[]> RESULT_ARRAY_COLUMNS = ParamInfoFactory
		.createParamInfo("result_array_columns", String[].class)
		.setDescription("columns whose value is double array, all columns that dim>1 are array " +
			"columns and can be omitted, columns that dim=1 but value is array must be set here")
		.setOptional()
		.setHasDefaultValue(new String[0]).build();

	default int[] getResultDim() {
		return get(RESULT_DIM);
	}

	default T setResultDim(int[] resultDim) {
		return set(RESULT_DIM, resultDim);
	}

	default String[] getResultArrayCols() {
		return get(RESULT_ARRAY_COLUMNS);
	}

	default T setResultArrayCols(String[] resultArrayCols) {
		return set(RESULT_ARRAY_COLUMNS, resultArrayCols);
	}
}
