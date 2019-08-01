package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @param <D>
 */
public class FlattenDataKey<D> extends RichFlatMapFunction<Tuple3<Long, String[], D>,
	Tuple2<Long, String>> {
	@Override
	public void flatMap(Tuple3<Long, String[], D> value,
		Collector<Tuple2<Long, String>> out) throws Exception {
		for (String k : value.f1) {
			out.collect(new Tuple2<>(value.f0, k));
		}
	}
}
