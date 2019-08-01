package org.apache.flink.ds.iter;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @param <M>
 * @param <F>
 */
public class ModelOrFeedbackKeySelector<M, F> implements
	KeySelector<ModelOrFeedback<M, F>, String> {
	private final KeySelector<M, String> modelKeySelector;
	private final KeySelector<F, String> feedbackKeySelector;

	public ModelOrFeedbackKeySelector(KeySelector<M, String> modelKeySelector,
		KeySelector<F, String> feedbackKeySelector) {
		this.modelKeySelector = modelKeySelector;
		this.feedbackKeySelector = feedbackKeySelector;
	}

	@Override
	public String getKey(ModelOrFeedback<M, F> value) throws Exception {
		return value.isModel ? modelKeySelector.getKey(value.model) :
			feedbackKeySelector.getKey(value.feedback);
	}
}
