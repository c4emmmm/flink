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

package org.apache.flink.table.ml.lib.pmml.appender;

import org.apache.flink.ml.api.core.Transformer;

import org.dmg.pmml.PMML;

/**
 * Base interface to append the operating information of each stage to the PMML.
 *
 * <p>Models supporting PMML exportation must have their appenders.
 *
 * <p>Pipeline can export as PMML only if all stages have their PMMLAppenders, so transformers
 * other than models may also need their PMMLAppenders.
 *
 * <p>Caution: This is an example. It's still now sure whether PMML is an appendable format while
 * building.
 */
public interface PMMLAppender<T extends Transformer<T>> {
	void append(T stage, PMML pmml);
}
