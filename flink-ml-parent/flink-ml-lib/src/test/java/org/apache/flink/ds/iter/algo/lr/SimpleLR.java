package org.apache.flink.ds.iter.algo.lr;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ds.iter.algo.SingleModelSupervisedIterativeAlgorithm;

import com.github.fommil.netlib.BLAS;
import com.google.gson.Gson;

import java.util.Arrays;

/**
 * model is so simple that can be put within one ps.
 */
public class SimpleLR implements
	SingleModelSupervisedIterativeAlgorithm<double[], Double, double[], double[], Double, DoubleAcc> {
	private long iter = 0;

	@Override
	public Double predict(double[] feature, double[] weights) {
		double dp = BLAS.getInstance().ddot(feature.length, feature, 1, weights, 1);
		return dp + weights[weights.length - 1];
	}

	@Override
	public Tuple2<double[], Double> train(double[] feature, Double label, double[] weights) {
		iter++;
		double learningRate = 0.1 / Math.sqrt((iter + 10) / 10);
		double pred = predict(feature, weights);
		double diff = pred - label;
		double loss = diff * diff;

		double multiplier = diff * -learningRate;
		double[] grad = new double[weights.length];
		Arrays.fill(grad, 0);
		BLAS.getInstance().daxpy(feature.length, multiplier, feature, 1, grad, 1);
		grad[feature.length] = multiplier;

		if (iter % 1000 == 0) {
			System.out.println("curModel=" + new Gson().toJson(weights) + ", iter=" + iter + ", " +
				"curLoss=" + loss);
		}
		return new Tuple2<>(grad, loss);
	}

	@Override
	public DoubleAcc createLossAcc() {
		return new DoubleAcc();
	}

	@Override
	public boolean isConverge(DoubleAcc lossAcc, Double loss) {
		lossAcc.value = lossAcc.value == 0 ? loss : lossAcc.value * 0.5 + loss * 0.5;
		boolean isConverge = lossAcc.value < 0.01;
		if (isConverge && lossAcc.sinceLastConverge > 10000) {
			lossAcc.sinceLastConverge = 0;
			return true;
		} else if (isConverge) {
			lossAcc.sinceLastConverge++;
		}
		return false;
	}

	@Override
	public double[] merge(double[] weights, double[] gradient) {
		int length = weights.length;
		double[] result = new double[length];
		System.arraycopy(weights, 0, result, 0, length);
		BLAS.getInstance().daxpy(length, 1, gradient, 1, result, 1);
		return result;
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
	public TypeInformation<Double> lossType() {
		return BasicTypeInfo.DOUBLE_TYPE_INFO;
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
