package org.apache.flink.ds.iter.struct;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class UpdateWrapper<M, U> implements MapFunction<U, UnifiedModelData<M, U>> {

	@Override
	public UnifiedModelData<M, U> map(U update) throws Exception {
		return UnifiedModelData.wrapUpdate(update);
	}

}
