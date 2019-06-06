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

package org.apache.flink.table.ml.lib.common.params.column;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * @param <T>
 */
public interface WithInputCol<T extends WithInputCol<T>> extends WithParams<T> {
	ParamInfo<String> INPUT_COL = ParamInfoFactory
		.createParamInfo("input_col", String.class)
		.setDescription("column of input data")
		.setRequired().build();

	default String getInputCol() {
		return get(INPUT_COL);
	}

	default T setInputCol(String inputCol) {
		return set(INPUT_COL, inputCol);
	}
}
