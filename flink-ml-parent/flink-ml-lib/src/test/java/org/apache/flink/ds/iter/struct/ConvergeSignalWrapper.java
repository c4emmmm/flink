package org.apache.flink.ds.iter.struct;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class ConvergeSignalWrapper<M, U> implements MapFunction<ConvergeSignal, UnifiedModelData<M, U>> {

	@Override
	public UnifiedModelData<M, U> map(ConvergeSignal signal) throws Exception {
		return UnifiedModelData.wrapConvergeSignal(signal);
	}
}
