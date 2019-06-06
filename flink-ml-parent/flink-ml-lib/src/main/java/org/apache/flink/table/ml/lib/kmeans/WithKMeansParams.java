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

package org.apache.flink.table.ml.lib.kmeans;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.ml.lib.common.params.column.WithInputCol;
import org.apache.flink.table.ml.lib.common.params.column.WithPredictionCol;
import org.apache.flink.table.ml.lib.common.params.iteration.WithMaxIter;
import org.apache.flink.table.ml.lib.common.params.iteration.WithTolerance;

/**
 *
 */
public interface WithKMeansParams<T extends WithKMeansParams<T>> extends WithParams<T>,
	WithInputCol<T>, WithPredictionCol<T>, WithMaxIter<T>, WithTolerance<T> {
	ParamInfo<Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("k")
		.setRequired()
		.setHasDefaultValue(2)
		.setValidator(value -> value >= 2).build();

	default T setK(int k) {
		return set(K, k);
	}

	default int getK() {
		return get(K);
	}
}
