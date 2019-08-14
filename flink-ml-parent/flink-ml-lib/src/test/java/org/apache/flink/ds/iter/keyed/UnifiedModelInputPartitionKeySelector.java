package org.apache.flink.ds.iter.keyed;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ds.iter.struct.UnifiedModelData;

/**
 * @param <M>
 * @param <U>
 */
public class UnifiedModelInputPartitionKeySelector<M, U> implements
	KeySelector<UnifiedModelData<M, U>, String> {
	private final KeySelector<M, String> modelKeySelector;
	private final KeySelector<U, String> updateKeySelector;

	public UnifiedModelInputPartitionKeySelector(KeySelector<M, String> modelKeySelector,
		KeySelector<U, String> updateKeySelector, int targetCount) {
		this.modelKeySelector = new PartitionKeySelector<>(modelKeySelector, targetCount);
		this.updateKeySelector = new PartitionKeySelector<>(updateKeySelector, targetCount);
	}

	@Override
	public String getKey(UnifiedModelData<M, U> value) throws Exception {
		if (value.isConvergeSignal) {
			return String.valueOf(value.convergeSignal.targetWorker);
		} else if (value.isModel) {
			return modelKeySelector.getKey(value.model);
		} else {
			//isUpdate
			return updateKeySelector.getKey(value.update);
		}
	}
}
