package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @param <F>
 */
public class FeedbackTailFlatMap<F> extends RichFlatMapFunction<F, F> {

	@Override
	public void flatMap(F value, Collector<F> out) throws Exception {
		FeedbackHeadFlatMap.queue.offer(value);
	}
}
