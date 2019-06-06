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

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Normalize a numerical column into the range [0, 1]. This is a packaging of Scale Transformer with
 * OutputMin and OutputMax fixed.
 */
public class Normalize implements Transformer<Normalize>, WithScaleParams<Normalize> {
	private final Scale scale = new Scale();

	{
		scale.setOutputMin(0);
		scale.setOutputMax(1);
	}

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		return scale.transform(tEnv, input);
	}

	@Override
	public Params getParams() {
		return scale.getParams();
	}
}
