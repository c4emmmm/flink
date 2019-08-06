package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class ModelWrapper<M, F>
	extends RichFlatMapFunction<M, ModelOrFeedback<M, F>> {
	@Override
	public void flatMap(M model, Collector<ModelOrFeedback<M, F>> out) throws Exception {
		out.collect(new ModelOrFeedback<>(true, model, null));
	}
}
