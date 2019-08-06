package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class UpdateWrapper<M, U> implements MapFunction<U, UnifiedModelInput<M, U>> {

	@Override
	public UnifiedModelInput<M, U> map(U update) throws Exception {
		return UnifiedModelInput.wrapUpdate(update);
	}

}
