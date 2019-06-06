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

/**
 *
 */
public class ExpressionUtil {

	public static String genUdfCallExpr(String udfName, String inputField, String resultField) {
		return genUdfCallExpr(udfName, new String[]{inputField}, resultField);
	}

	public static String genUdfCallExpr(String udfName, String[] inputFields, String resultField) {
		StringBuilder expr = new StringBuilder(udfName).append("(");
		for (int i = 0; i < inputFields.length; i++) {
			if (i != 0) {
				expr.append(", ");
			}
			expr.append(inputFields[i]);
		}
		return expr.append(") as ").append(resultField).toString();
	}

	public static String genAppendUdfCallExpr(String udfName, String udfInputField, String udfResultField, String... allInputFields) {
		return genAppendUdfCallExpr(
			udfName, new String[]{udfInputField}, udfResultField, allInputFields);
	}

	public static String genAppendUdfCallExpr(String udfName, String[] udfInputFields, String udfResultField, String... allInputFields) {
		StringBuilder expr = new StringBuilder();
		for (String field : allInputFields) {
			if (field.equals(udfResultField)) {
				continue;
			}
			expr.append(field).append(", ");
		}
		return expr.append(genUdfCallExpr(udfName, udfInputFields, udfResultField)).toString();
	}

	public static String genSelectAllExpr(String[] fields) {
		StringBuilder expr = new StringBuilder();
		for (String field : fields) {
			if (expr.length() != 0) {
				expr.append(", ");
			}
			expr.append(field);
		}
		return expr.toString();
	}
}
