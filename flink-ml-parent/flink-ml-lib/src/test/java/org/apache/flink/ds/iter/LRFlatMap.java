package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.gson.Gson;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.Map;

/**
 *
 */
public class LRFlatMap extends
	RichFlatMapFunction<Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>>,
		Tuple2<Integer, Double>> {

	@Override
	public void flatMap(Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>> value,
		Collector<Tuple2<Integer, Double>> out) throws Exception {
		double[] weights = new double[value.f0.f0.length + 1];
		for (Tuple2<Integer, Double> m : value.f1.values()) {
			weights[m.f0] = m.f1;
		}
		System.out.println("model:" + new Gson().toJson(weights));
		Tuple3<double[], double[], Double> v = new Tuple3<>(value.f0.f0, weights,
			value.f0.f1);
		RealVector data = new ArrayRealVector(v.f0).append(1);
		RealVector w = new ArrayRealVector(v.f1);
		double label = v.f2;
		double pred = data.dotProduct(w);
		double loss = pred - label;
		double[] grad = data.mapMultiply(loss / pred).toArray();
		System.out.println("grad:" + new Gson().toJson(grad));
		for (int i = 0; i < grad.length; i++) {
			out.collect(new Tuple2<>(i, grad[i]));
		}
	}
}
