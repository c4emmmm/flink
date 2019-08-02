package org.apache.flink.ds.iter.broadcast;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ds.iter.ModelOrFeedback;
import org.apache.flink.ds.iter.ModelOrFeedbackKeySelector;
import org.apache.flink.ds.iter.PsMerger;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * @param <M>
 * @param <F>
 */
public class BroadcastPsCoProcessor<D, M, F> extends
	RichCoFlatMapFunction<ModelOrFeedback<M, F>, D, Tuple2<D, Map<String, M>>> implements
	CheckpointedFunction {
	private PsMerger<M, F> merger;
	private ModelOrFeedbackKeySelector<M, F> modelKeySelector;
	private KeySelector<D, String[]> dataKeySelector;

	private Map<String, M> state = new HashMap<>();
	private int workerId = -1;

	public BroadcastPsCoProcessor(PsMerger<M, F> merger,
		ModelOrFeedbackKeySelector<M, F> modelKeySelector,
		KeySelector<D, String[]> dataKeySelector) {
		this.merger = merger;
		this.modelKeySelector = modelKeySelector;
		this.dataKeySelector = dataKeySelector;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		workerId = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void flatMap1(ModelOrFeedback<M, F> value,
		Collector<Tuple2<D, Map<String, M>>> out) throws Exception {
		if (value.isModel) {
			state.put(modelKeySelector.getKey(value), value.model);
		} else {
			state.put(modelKeySelector.getKey(value),
				merger.merge(state.get(modelKeySelector.getKey(value)), value.feedback));
		}
		System.out.println(workerId + " curModel:" + new Gson().toJson(state));
	}

	@Override
	public void flatMap2(D value, Collector<Tuple2<D, Map<String, M>>> out) throws Exception {
		String[] keys = dataKeySelector.getKey(value);
		Map<String, M> model = new HashMap<>();
		for (String k : keys) {
			model.put(k, state.get(k));
		}
		if (model.isEmpty()) {
			return;
		}
		out.collect(new Tuple2<>(value, model));
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//save stateMap
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		//load stateMap
	}
}
