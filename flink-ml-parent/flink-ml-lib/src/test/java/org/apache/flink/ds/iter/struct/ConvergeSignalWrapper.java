package org.apache.flink.ds.iter.struct;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class ConvergeSignalWrapper<M, U> implements MapFunction<ConvergeSignal, UnifiedModelInput<M, U>> {

	@Override
	public UnifiedModelInput<M, U> map(ConvergeSignal signal) throws Exception {
		return UnifiedModelInput.wrapConvergeSignal(signal);
	}
}
