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

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.ml.lib.common.params.column.WithPredictionCol;
import org.apache.flink.table.ml.lib.common.params.model.WithModelFile;

/**
 * @param <T>
 */
public interface WithALSBaseParams<T extends WithALSBaseParams<T>> extends WithParams<T>,
	WithPredictionCol<T>, WithModelFile<T> {
	ParamInfo<String> USER_COL = ParamInfoFactory
		.createParamInfo("user_col", String.class)
		.setDescription("column of user")
		.setRequired().build();

	ParamInfo<String> ITEM_COL = ParamInfoFactory
		.createParamInfo("item_col", String.class)
		.setDescription("column of item")
		.setRequired().build();

	default String getUserCol() {
		return get(USER_COL);
	}

	default T setUserCol(String userCol) {
		return set(USER_COL, userCol);
	}

	default String getItemCol() {
		return get(ITEM_COL);
	}

	default T setItemCol(String itemCol) {
		return set(ITEM_COL, itemCol);
	}
}
