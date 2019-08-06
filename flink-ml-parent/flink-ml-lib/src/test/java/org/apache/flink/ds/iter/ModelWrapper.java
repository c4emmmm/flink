package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class ModelWrapper<M, F> implements MapFunction<M, UnifiedModelInput<M, F>> {

	@Override
	public UnifiedModelInput<M, F> map(M model) throws Exception {
		return UnifiedModelInput.wrapModel(model);
	}
}
