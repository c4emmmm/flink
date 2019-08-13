package org.apache.flink.ds.iter.test.lr;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ds.iter.test.base.SupervisedIterativeAlgorithm;

import com.google.gson.Gson;
import org.apache.commons.math3.linear.ArrayRealVector;

/**
 *
 */
public class LRIterativeAlgorithm
	implements SupervisedIterativeAlgorithm<double[], double[], double[], Double> {

	int inputCount = 0;
	int convergeCount = 0;
	boolean firstOutput = false;

	@Override
	public boolean isConverge(double[] gradient) {
		inputCount++;
		if (inputCount % 500 == 0) {
			System.out.println(
				"curIter:" + inputCount + ", cur count=" + convergeCount + ", cur loss?=" +
					new ArrayRealVector(gradient).getNorm() + ", cur graident=" +
					new Gson().toJson(gradient));
		}
		if (new ArrayRealVector(gradient).getNorm() < 0.01) {
			convergeCount++;
		} else {
			convergeCount = convergeCount <= 1 ? 0 : convergeCount - 1;
		}
		if (convergeCount == 1000 && !firstOutput) {
			System.out.println("iter=" + inputCount + ", seems converge");
			firstOutput = true;
			inputCount = 0;
			return true;
		}
		if (firstOutput && convergeCount > 1000 && inputCount > 10000) {
			System.out.println("iter=" + inputCount + ", output a new version");
			inputCount = 0;
			return true;
		}
		return false;
	}

	@Override
	public double[] merge(double[] weight, double[] gradient) {
		return new ArrayRealVector(weight).add(new ArrayRealVector(gradient)).toArray();
	}

	@Override
	public Double infer(double[] feature, Double label, double[] weight) {
		double result =
			new ArrayRealVector(feature).append(1).dotProduct(new ArrayRealVector(weight));
		if (Math.random() > 0.999) {
			System.out.println(
				"input=" + new Gson().toJson(feature) + ", label=" + label + ", \nweight=" +
					new Gson().toJson(weight) + ", result=" + result);
		}
		return result;
	}

	private int iter = 10;

	@Override
	public double[] train(double[] feature, Double label, double[] weight) {
		iter++;
		double learningRate = 0.1 / Math.sqrt(iter / 10.0);
		double pred = infer(feature, label, weight);
		double diff = pred - label;
		return new ArrayRealVector(feature).append(1)
			.mapMultiply(diff).mapMultiply(-learningRate).toArray();
	}

	@Override
	public TypeInformation<double[]> modelType() {
		return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
	}

	@Override
	public TypeInformation<double[]> updateType() {
		return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
	}

	@Override
	public TypeInformation<double[]> featureType() {
		return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
	}

	@Override
	public TypeInformation<Double> labelType() {
		return BasicTypeInfo.DOUBLE_TYPE_INFO;
	}
}
