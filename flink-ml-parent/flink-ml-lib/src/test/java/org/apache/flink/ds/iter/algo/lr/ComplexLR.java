package org.apache.flink.ds.iter.algo.lr;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.ds.iter.algo.SupervisedIterativeAlgorithm;

import com.github.fommil.netlib.BLAS;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * assume model is too large to put within one ps, has to be separated by key. data has to be
 * changed to sparseVector in this case
 */
public class ComplexLR implements
	SupervisedIterativeAlgorithm<double[], Double, Tuple2<Integer, Double>, Tuple2<Integer, Double>, Double,
		DoubleAcc> {
	private long iter = 0;

	@Override
	public Double predict(double[] feature, Map<String, Tuple2<Integer, Double>> model) {
		double[] weights = parseWeights(feature.length + 1, model);
		return weights == null ? null : predictWithWeights(feature, weights);
	}

	private double predictWithWeights(double[] feature, double[] weights) {
		double dp = BLAS.getInstance().ddot(feature.length, feature, 1, weights, 1);
		return dp + weights[weights.length - 1];
	}

	@Override
	public Tuple2<Iterable<Tuple2<Integer, Double>>, Double> train(double[] feature,
		Double label,
		Map<String, Tuple2<Integer, Double>> model) {
		double[] weights = parseWeights(feature.length + 1, model);
		if (weights == null) {
			return null;
		}

		iter++;
		double learningRate = 0.1 / Math.sqrt((iter + 10) / 10);
		double pred = predictWithWeights(feature, weights);
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

		List<Tuple2<Integer, Double>> updates = new ArrayList<>(weights.length);
		for (int i = 0; i < weights.length; i++) {
			updates.add(new Tuple2<>(i, grad[i]));
		}
		return new Tuple2<>(updates, loss);
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
	public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> weight,
		Tuple2<Integer, Double> grad) {
		return new Tuple2<>(weight.f0, weight.f1 + grad.f1);
	}

	@Override
	public TypeInformation<Tuple2<Integer, Double>> modelType() {
		return new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);
	}

	@Override
	public KeySelector<Tuple2<Integer, Double>, String> modelKeySelector() {
		return (KeySelector<Tuple2<Integer, Double>, String>) value -> String.valueOf(value.f0);
	}

	@Override
	public TypeInformation<Tuple2<Integer, Double>> updateType() {
		return new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);
	}

	@Override
	public KeySelector<Tuple2<Integer, Double>, String> updateKeySelector() {
		return (KeySelector<Tuple2<Integer, Double>, String>) value -> String.valueOf(value.f0);
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

	@Override
	public KeySelector<double[], String[]> featureKeySelector() {
		return new KeySelector<double[], String[]>() {
			int size = -1;
			String[] cache;

			@Override
			public String[] getKey(double[] value) throws Exception {
				if (value.length == size) {
					return cache;
				}
				size = value.length;
				cache = new String[size + 1];
				for (int i = 0; i < size + 1; i++) {
					cache[i] = String.valueOf(i);
				}
				return cache;
			}
		};
	}

	private double[] parseWeights(int size, Map<String, Tuple2<Integer, Double>> model) {
		double[] weights = new double[size];
		for (int i = 0; i < size; i++) {
			Tuple2<Integer, Double> m = model.get(String.valueOf(i));
			if (m == null) {
				return null;
			}
			weights[i] = m.f1;
		}
		return weights;
	}
}
