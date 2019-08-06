package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class FlagToConvergeSignal implements MapFunction<Boolean, ConvergeSignal> {
	@Override
	public ConvergeSignal map(Boolean isConverge) throws Exception {
		ConvergeSignal signal = new ConvergeSignal();
		signal.isConverge = isConverge;
		signal.targetWorker = 0;
		return signal;
	}
}
