package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class BroadcastConvergeSignal implements FlatMapFunction<ConvergeSignal, ConvergeSignal> {
	private final int psParallelism;

	public BroadcastConvergeSignal(int psParallelism) {
		this.psParallelism = psParallelism;
	}

	@Override
	public void flatMap(ConvergeSignal value, Collector<ConvergeSignal> out) throws Exception {
		for (int i = 0; i < psParallelism; i++) {
			ConvergeSignal signal = new ConvergeSignal();
			signal.targetWorker = i;
			signal.isConverge = value.isConverge;
			signal.versionId = value.versionId;
			out.collect(signal);
		}
	}
}
