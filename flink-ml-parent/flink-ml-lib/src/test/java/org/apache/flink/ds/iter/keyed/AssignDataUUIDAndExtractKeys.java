package org.apache.flink.ds.iter.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @param <D>
 */
public class AssignDataUUIDAndExtractKeys<D> extends
	RichFlatMapFunction<D, Tuple3<Long, String[], D>> {
	private KeySelector<D, String[]> keyExtractor;
	private int workerId;

	public AssignDataUUIDAndExtractKeys(KeySelector<D, String[]> keyExtractor) {
		this.keyExtractor = keyExtractor;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		workerId = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void flatMap(D value, Collector<Tuple3<Long, String[], D>> out) throws Exception {
		out.collect(new Tuple3<>(nextId(), keyExtractor.getKey(value), value));
	}

	private long nextId() {
		//TODO not good
		return System.nanoTime() * 100000 + workerId;
	}
}
