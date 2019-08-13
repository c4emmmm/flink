package org.apache.flink.ds.iter.test.base;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @param <IN>
 */
public class UniqueKeySelector<IN> implements KeySelector<IN, String> {
	static final String UNIQUE_MODEL_KEY = "unique_model_key";

	@Override
	public String getKey(IN value) throws Exception {
		return UNIQUE_MODEL_KEY;
	}
}
