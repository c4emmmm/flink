package org.apache.flink.ds.iter;

import java.io.Serializable;

/**
	 * @param <M>
	 * @param <F>
	 */
	public class ModelOrFeedback<M, F> implements Serializable {
		boolean isModel; //or feedback
		M model;
		F feedback;

		public ModelOrFeedback(boolean isModel, M model, F feedback) {
			this.isModel = isModel;
			this.model = model;
			this.feedback = feedback;
		}

	}
