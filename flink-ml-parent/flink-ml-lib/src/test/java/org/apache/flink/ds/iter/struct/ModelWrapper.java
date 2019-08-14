package org.apache.flink.ds.iter.struct;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class ModelWrapper<M, F> implements MapFunction<M, UnifiedModelData<M, F>> {

	@Override
	public UnifiedModelData<M, F> map(M model) throws Exception {
		return UnifiedModelData.wrapModel(model);
	}
}
