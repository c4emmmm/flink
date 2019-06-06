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

package org.apache.flink.table.ml.lib.gradient.descent;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.ml.lib.common.params.iteration.WithMaxIter;
import org.apache.flink.table.ml.lib.common.params.iteration.WithTolerance;

/**
 *
 */
public interface WithGradientDescentParams<T extends WithGradientDescentParams<T>> extends
	WithParams<T>, WithMaxIter<T>, WithTolerance<T> {
	ParamInfo<Integer> DIM = ParamInfoFactory
		.createParamInfo("dim", Integer.class)
		.setDescription("dim")
		.setRequired().build();
	ParamInfo<Gradient.Type> GRADIENT_TYPE = ParamInfoFactory
		.createParamInfo("gradient_type", Gradient.Type.class)
		.setDescription("gradientType: LEAST_SQUARE, LOGISTIC, CUSTOMIZE")
		.setRequired()
		.setHasDefaultValue(Gradient.Type.LEAST_SQUARE).build();
	ParamInfo<String> GRADIENT_CLASS = ParamInfoFactory
		.createParamInfo("gradient_class", String.class)
		.setDescription("customized gradient class name, use when gradientType=CUSTOMIZE")
		.setOptional().build();
	ParamInfo<Double> INIT_LEARNING_RATE = ParamInfoFactory
		.createParamInfo("learning_rate", Double.class)
		.setDescription("initialized learning rate, effectiveLearningRate = r / sqrt(iter)")
		.setRequired()
		.setHasDefaultValue(0.1).build();
	ParamInfo<Double> L1_REGULARIZATION = ParamInfoFactory
		.createParamInfo("l1_regularization", Double.class)
		.setDescription("L1 regularization, default 0")
		.setOptional()
		.setHasDefaultValue(0.0).build();
	ParamInfo<Double> L2_REGULARIZATION = ParamInfoFactory
		.createParamInfo("l2_regularization", Double.class)
		.setDescription("L1 regularization, default 0")
		.setOptional()
		.setHasDefaultValue(0.0).build();

	default int getDim() {
		return get(DIM);
	}

	default T setDim(int dim) {
		return set(DIM, dim);
	}

	default Gradient.Type getGradientType() {
		return get(GRADIENT_TYPE);
	}

	default T setGradientType(Gradient.Type t) {
		return set(GRADIENT_TYPE, t);
	}

	default String getGradientClass() {
		return get(GRADIENT_CLASS);
	}

	default T setGradientClass(String c) {
		return set(GRADIENT_CLASS, c);
	}

	default double getInitLearningRate() {
		return get(INIT_LEARNING_RATE);
	}

	default T setInitLearningRate(double r) {
		return set(INIT_LEARNING_RATE, r);
	}

	default double getL1Regularization() {
		return get(L1_REGULARIZATION);
	}

	default T setL1Regularization(double r) {
		return set(L1_REGULARIZATION, r);
	}

	default double getL2Regularization() {
		return get(L2_REGULARIZATION);
	}

	default T setL2Regularization(double r) {
		return set(L2_REGULARIZATION, r);
	}
}
