package org.apache.flink.ds.iter.test.base;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @param <IN>
 */
public class UniqueKeyArrSelector<IN> implements KeySelector<IN, String[]> {

	@Override
	public String[] getKey(IN value) throws Exception {
		return new String[]{UniqueKeySelector.UNIQUE_MODEL_KEY};
	}
}
