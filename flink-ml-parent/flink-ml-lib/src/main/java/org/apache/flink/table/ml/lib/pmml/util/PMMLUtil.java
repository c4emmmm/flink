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

package org.apache.flink.table.ml.lib.pmml.util;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TransformationDictionary;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class PMMLUtil {

	public static String readFile(String path) throws IOException {
		try (BufferedReader r = new BufferedReader(new FileReader(path))) {
			StringBuilder sb = new StringBuilder();
			while (r.ready()) {
				sb.append(r.readLine());
			}
			return sb.toString();
		}
	}

	public static void writeFile(String content, String path) throws IOException {
		try (BufferedWriter w = new BufferedWriter(new FileWriter(path))) {
			w.write(content);
		}
	}

	public static PMML readPMMLFile(String path) throws IOException {
		return parsePMMLString(readFile(path));
	}

	public static void writePMMLFile(PMML pmml, String path) throws IOException {
		writeFile(toPMMLString(pmml), path);
	}

	public static PMML parsePMMLString(String pmml) {
		try {
			try (ByteArrayInputStream is = new ByteArrayInputStream(pmml.getBytes())) {
				try {
					return org.jpmml.model.PMMLUtil.unmarshal(is);
				} catch (SAXException | JAXBException e) {
					throw new IOException(e);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String toPMMLString(PMML pmml) {
		try {
			try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
				try {
					org.jpmml.model.PMMLUtil.marshal(pmml, os);
				} catch (JAXBException e) {
					throw new IOException(e);
				}
				return os.toString();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Evaluator createEvaluator(PMML pmml) {
		ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
		return modelEvaluatorFactory.newModelEvaluator(pmml);
	}

	public static DataDictionary getDataDictionary(PMML pmml) {
		DataDictionary dd = pmml.getDataDictionary();
		if (dd == null) {
			dd = new DataDictionary();
			pmml.setDataDictionary(dd);
		}
		return dd;
	}

	public static TransformationDictionary getTransformationDictionary(PMML pmml) {
		TransformationDictionary td = pmml.getTransformationDictionary();
		if (td == null) {
			td = new TransformationDictionary();
			pmml.setTransformationDictionary(td);
		}
		return td;
	}

	public static void registerDataFieldIfAbsent(PMML pmml, FieldName inputCol, OpType opType, DataType dataType) {
		if (!PMMLUtil.containsField(pmml, inputCol.getValue())) {
			getDataDictionary(pmml).addDataFields(new DataField(inputCol, opType, dataType));
		}
	}

	public static boolean containsField(PMML pmml, String inputCol) {
		return getDataDictionary(pmml).getDataFields().stream()
			.map(f -> f.getName().getValue()).anyMatch(f -> f.equals(inputCol)) ||
			getTransformationDictionary(pmml).getDerivedFields().stream()
				.map(f -> f.getName().getValue()).anyMatch(f -> f.equals(inputCol));
	}

	public static void registerMiningSchemaInput(PMML pmml, MiningSchema miningSchema, String[] exceptionFields) {
		Set<String> exceptionFieldSet = new HashSet<>(Arrays.asList(exceptionFields));
		for (DataField f : getDataDictionary(pmml).getDataFields()) {
			if (!exceptionFieldSet.contains(f.getName().getValue())) {
				miningSchema.addMiningFields(
					new MiningField(f.getName()).setUsageType(MiningField.UsageType.ACTIVE));
			}
		}
	}
}
