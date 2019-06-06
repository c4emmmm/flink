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
import org.apache.flink.table.ml.lib.common.params.column.WithLabelCol;
import org.apache.flink.table.ml.lib.common.params.iteration.WithMaxIter;
import org.apache.flink.table.ml.lib.common.params.iteration.WithTolerance;

/**
 * @param <T>
 */
public interface WithALSEstimtorParams<T extends WithALSEstimtorParams<T>> extends WithParams<T>,
	WithALSBaseParams<T>, WithMaxIter<T>, WithTolerance<T>, WithLabelCol<T> {
	ParamInfo<Integer> RANK = ParamInfoFactory
		.createParamInfo("rank", Integer.class)
		.setDescription("rank of factor")
		.setRequired()
		.setHasDefaultValue(3)
		.setValidator(v -> v > 0).build();
	ParamInfo<Double> REG_LAMBDA = ParamInfoFactory
		.createParamInfo("lambda", Double.class)
		.setDescription("lambda for regularization")
		.setRequired()
		.setHasDefaultValue(0.1).build();

	default T setRank(int rank) {
		return set(RANK, rank);
	}

	default int getRank() {
		return get(RANK);
	}

	default T setLambda(double lambda) {
		return set(REG_LAMBDA, lambda);
	}

	default double getLambda() {
		return get(REG_LAMBDA);
	}
}
