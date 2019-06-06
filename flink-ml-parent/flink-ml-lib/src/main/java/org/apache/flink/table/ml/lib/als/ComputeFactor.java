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

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

/**
 *
 */
public class ComputeFactor extends ScalarFunction {

	public double[] eval(double[][] sumVtv, double[] sumVr, double lambda) {
		assert (sumVtv.length == sumVtv[0].length && sumVtv.length == sumVr.length);
		//solve m * x= y, with LUDecomposition, where m = sumVtv, y = sumVr
		RealMatrix m = new Array2DRowRealMatrix(sumVtv);
		m = m.add(MatrixUtils.createRealIdentityMatrix(sumVtv.length).scalarMultiply(lambda));
		DecompositionSolver solver = new LUDecomposition(m).getSolver();
		return solver.solve(new ArrayRealVector(sumVr)).toArray();
	}
}
