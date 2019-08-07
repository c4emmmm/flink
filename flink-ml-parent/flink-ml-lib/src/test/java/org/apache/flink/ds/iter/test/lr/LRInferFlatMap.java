package org.apache.flink.ds.iter.test.lr;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import org.apache.commons.math3.linear.ArrayRealVector;

import java.util.Map;

/**
 *
 */
public class LRInferFlatMap extends
	RichFlatMapFunction<Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>>,
		Tuple3<double[], Double, Double>> {
	@Override
	public void flatMap(Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>> value,
		Collector<Tuple3<double[], Double, Double>> out) throws Exception {
		double[] data = value.f0.f0;
		double[] weight = parseWeight(data.length, value.f1);

		//ignore illegal data or model
		if (weight == null) {
			return;
		}

		out.collect(new Tuple3<>(value.f0.f0, value.f0.f1,
			new ArrayRealVector(data).append(1).dotProduct(new ArrayRealVector(weight))));
	}

	private double[] parseWeight(int size, Map<String, Tuple2<Integer, Double>> model) {
		double[] weights = new double[size + 1];
		for (int i = 0; i < size + 1; i++) {
			Tuple2<Integer, Double> m = model.get(String.valueOf(i));
			if (m == null) {
				//illegal data size or not fulfilled model
				return null;
			}
			weights[i] = m.f1;
		}
		return weights;
	}
}
