package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class FeedbackWrapper<M, F>
	extends RichFlatMapFunction<F, ModelOrFeedback<M, F>> {
	@Override
	public void flatMap(F feedback, Collector<ModelOrFeedback<M, F>> out) throws Exception {
		out.collect(new ModelOrFeedback<>(false, null, feedback));
	}
}
