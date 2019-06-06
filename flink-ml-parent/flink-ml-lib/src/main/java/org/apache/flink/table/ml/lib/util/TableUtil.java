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

package org.apache.flink.table.ml.lib.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 *
 */
public class TableUtil {

	public static void assertBatchTableEnvironment(TableEnvironment tEnv) {
		assert tEnv instanceof org.apache.flink.table.api.java.BatchTableEnvironment ||
			tEnv instanceof org.apache.flink.table.api.scala.BatchTableEnvironment;
	}

	public static void assertStreamTableEnvironment(TableEnvironment tEnv) {
		assert tEnv instanceof org.apache.flink.table.api.java.StreamTableEnvironment ||
			tEnv instanceof org.apache.flink.table.api.scala.StreamTableEnvironment;
	}

	public static DataSet<Row> toDataSet(TableEnvironment tEnv, Table table) {
		assertBatchTableEnvironment(tEnv);
		TableSchema schema = table.getSchema();
		RowTypeInfo rowTypeInfo = new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());

		DataSet<Row> set;
		if (tEnv instanceof org.apache.flink.table.api.java.BatchTableEnvironment) {
			set = ((org.apache.flink.table.api.java.BatchTableEnvironment) tEnv)
				.toDataSet(table, rowTypeInfo);
		} else {
			//scala env
			set = ((org.apache.flink.table.api.scala.BatchTableEnvironment) tEnv)
				.toDataSet(table, rowTypeInfo).javaSet();
		}
		return set;
	}

	public static <T, ACC> void registerAggregateFunction(TableEnvironment tEnv, String name, AggregateFunction<T, ACC> agg) {
		TypeInformation<T> typeInfo = TypeExtractor
			.createTypeInfo(agg, AggregateFunction.class, agg.getClass(), 0);
		TypeInformation<ACC> accTypeInfo = TypeExtractor
			.createTypeInfo(agg, AggregateFunction.class, agg.getClass(), 1);

		if (tEnv instanceof org.apache.flink.table.api.java.BatchTableEnvironment) {
			((org.apache.flink.table.api.java.BatchTableEnvironment) tEnv)
				.registerFunction(name, agg);
		} else if (tEnv instanceof org.apache.flink.table.api.scala.BatchTableEnvironment) {
			((org.apache.flink.table.api.scala.BatchTableEnvironment) tEnv)
				.registerFunction(name, agg, typeInfo, accTypeInfo);
		} else if (tEnv instanceof org.apache.flink.table.api.java.StreamTableEnvironment) {
			((org.apache.flink.table.api.java.StreamTableEnvironment) tEnv)
				.registerFunction(name, agg);
		} else {
			//scala stream env
			((org.apache.flink.table.api.scala.StreamTableEnvironment) tEnv)
				.registerFunction(name, agg, typeInfo, accTypeInfo);
		}
	}

	public static <T> void registerTableFunction(TableEnvironment tEnv, String name, TableFunction<T> func) {
		TypeInformation<T> typeInfo = TypeExtractor
			.createTypeInfo(func, TableFunction.class, func.getClass(), 0);

		if (tEnv instanceof org.apache.flink.table.api.java.BatchTableEnvironment) {
			((org.apache.flink.table.api.java.BatchTableEnvironment) tEnv)
				.registerFunction(name, func);
		} else if (tEnv instanceof org.apache.flink.table.api.scala.BatchTableEnvironment) {
			((org.apache.flink.table.api.scala.BatchTableEnvironment) tEnv)
				.registerFunction(name, func, typeInfo);
		} else if (tEnv instanceof org.apache.flink.table.api.java.StreamTableEnvironment) {
			((org.apache.flink.table.api.java.StreamTableEnvironment) tEnv)
				.registerFunction(name, func);
		} else {
			//scala stream env
			((org.apache.flink.table.api.scala.StreamTableEnvironment) tEnv)
				.registerFunction(name, func, typeInfo);
		}
	}
}
