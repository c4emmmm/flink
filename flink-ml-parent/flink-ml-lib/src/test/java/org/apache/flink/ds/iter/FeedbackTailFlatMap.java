package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @param <F>
 */
public class FeedbackTailFlatMap<F> extends RichFlatMapFunction<F, F> {

	public int workerId = -1;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		workerId = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void flatMap(F value, Collector<F> out) throws Exception {
		FeedbackHeadFlatMap.getWorkerQueue(workerId).offer(value);
	}
}
