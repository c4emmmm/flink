package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @param <D>
 */
public class DataUUIDAssigner<D> extends
	RichFlatMapFunction<D, Tuple3<Long, String[], D>> {
	KeySelector<D, String[]> keyExtractor;

	public DataUUIDAssigner(KeySelector<D, String[]> keyExtractor) {
		this.keyExtractor = keyExtractor;
	}

	@Override
	public void flatMap(D value, Collector<Tuple3<Long, String[], D>> out) throws Exception {
		long id = nextId();
		out.collect(new Tuple3<>(id, keyExtractor.getKey(value), value));
	}

	private long nextId() {
		return System.nanoTime() * 1000 + (long) Math.floor(Math.random() * 1000);
	}
}
