package org.apache.flink.ds.iter;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @param <IN>
 */
public class PartitionKeySelector<IN> implements
	KeySelector<IN, String> {

	private final int targetCount;
	private final KeySelector<IN, String> innerSelector;

	public PartitionKeySelector(KeySelector<IN, String> innerSelector, int targetCount) {
		this.innerSelector = innerSelector;
		this.targetCount = targetCount;
	}

	@Override
	public String getKey(IN value) throws Exception {
		//assume that integer key can send message to the target worker with the specific id
		return String.valueOf(Math.abs(innerSelector.getKey(value).hashCode() % targetCount));
	}
}
