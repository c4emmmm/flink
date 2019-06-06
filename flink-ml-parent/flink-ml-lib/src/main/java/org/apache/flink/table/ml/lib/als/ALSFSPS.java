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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.ml.lib.common.functions.serialize.DoubleArrayStringConverter;
import org.apache.flink.table.ml.lib.util.ExpressionUtil;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.table.ml.lib.als.ALSConstants.FACTOR_FILE_DELIMITER;
import static org.apache.flink.table.ml.lib.als.ALSConstants.X_FACTOR_FILE;
import static org.apache.flink.table.ml.lib.als.ALSConstants.Y_FACTOR_FILE;

/**
 *
 */
public class ALSFSPS {
	private static Table getFactorSource(TableEnvironment tEnv, String filePath, String fileName) {
		CsvTableSource source = CsvTableSource.builder()
			.path(filePath + "/" + fileName)
			.field("factor_id", Types.LONG)
			.field("str_factor", Types.STRING)
			.fieldDelimiter(FACTOR_FILE_DELIMITER).build();
		String sourceName = "factorSource_" + fileName + "_" + System.nanoTime();

		ScalarFunction deserialize = new DoubleArrayStringConverter();
		String funcName = "deserializeArray_" + fileName + "_" + System.nanoTime();

		tEnv.registerTableSource(sourceName, source);
		tEnv.registerFunction(funcName, deserialize);

		String selectExpr = ExpressionUtil.genAppendUdfCallExpr(funcName, "str_factor", "factor",
			"factor_id", "str_factor");
		return tEnv.scan(sourceName).select(selectExpr);
	}

	private static void writeFactor(Table factors, BatchTableEnvironment tEnv, String filePath, String fileName) {
		ScalarFunction serialize = new DoubleArrayStringConverter();
		String funcName = "serializeArray_" + fileName + "_" + System.nanoTime();

		TableSink<Row> sink = new CsvTableSink(filePath + "/" + fileName,
			FACTOR_FILE_DELIMITER, 1, FileSystem.WriteMode.OVERWRITE)
			.configure(new String[]{"factor_id", "str_factor"},
				new TypeInformation<?>[]{Types.LONG, Types.STRING});
		String sinkName = "factorSink_" + fileName + "_" + System.nanoTime();

		tEnv.registerFunction(funcName, serialize);
		tEnv.registerTableSink(sinkName, sink);

		String funcCall = ExpressionUtil.genUdfCallExpr(funcName, "factor", "str_factor");
		factors.select("factor_id, " + funcCall).insertInto(sinkName);

		try {
			//TODO: replace with a better way
			tEnv.toDataSet(factors, Row.class)
				.getExecutionEnvironment()
				.execute();
		} catch (Exception e) {
			throw new RuntimeException("Failed to execute writing factors to " + fileName, e);
		}
	}

	public static Table getXFactor(TableEnvironment tEnv, String modelFilePath) {
		return getFactorSource(tEnv, modelFilePath, X_FACTOR_FILE)
			.select("factor_id as x_id, factor as " + "x_fac");
	}

	public static Table getYFactor(TableEnvironment tEnv, String modelFilePath) {
		return getFactorSource(tEnv, modelFilePath, Y_FACTOR_FILE)
			.select("factor_id as y_id, factor as y_fac");
	}

	public static void writeXFactor(Table xFactor, BatchTableEnvironment tEnv, String modelFilePath) {
		Table renamedTable = xFactor.select("x_id as factor_id, x_fac as factor");
		writeFactor(renamedTable, tEnv, modelFilePath, X_FACTOR_FILE);
	}

	public static void writeYFactor(Table yFactor, BatchTableEnvironment tEnv, String modelFilePath) {
		Table renamedTable = yFactor.select("y_id as factor_id, y_fac as factor");
		writeFactor(renamedTable, tEnv, modelFilePath, Y_FACTOR_FILE);
	}
}
