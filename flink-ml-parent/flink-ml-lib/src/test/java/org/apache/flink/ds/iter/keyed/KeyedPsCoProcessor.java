package org.apache.flink.ds.iter.keyed;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ds.iter.PsMerger;
import org.apache.flink.ds.iter.UnifiedModelInput;
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
 * @param <U>
 */
public class KeyedPsCoProcessor<M, U> extends
	RichCoFlatMapFunction<UnifiedModelInput<M, U>, Tuple2<Long, String>, Tuple3<Long, String, M>> implements
	CheckpointedFunction {
	private PsMerger<M, U> merger;
	private KeySelector<M, String> modelKeySelector;
	private KeySelector<U, String> updateKeySelector;

	private TypeInformation<M> modelType;
	private Map<String, M> state = new HashMap<>();

	private int workerId = -1;

	public KeyedPsCoProcessor(PsMerger<M, U> merger, KeySelector<M, String> modelKeySelector,
		KeySelector<U, String> updateKeySelector,
		TypeInformation<M> modelType) {
		this.merger = merger;
		this.modelKeySelector = modelKeySelector;
		this.updateKeySelector = updateKeySelector;
		this.modelType = modelType;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		workerId = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void flatMap1(UnifiedModelInput<M, U> value, Collector<Tuple3<Long, String, M>> out)
		throws Exception {
		if (value.isModel) {
			state.put(modelKeySelector.getKey(value.model), value.model);
			System.out.println("ps" + workerId + ":init model:");
			for (Map.Entry<String, M> e : state.entrySet()) {
				System.out.println(
					"ps" + workerId + ":" + e.getKey() + "=" + new Gson().toJson(e.getValue()));
			}
		} else if (value.isUpdate) {
			String key = updateKeySelector.getKey(value.update);
			state.put(key, merger.merge(state.get(key), value.update));
		} else {
			//value.isConvergeSignal
			//iterate on model and collect all kvs as side output
			System.out.println("ps" + workerId + ":output model:");
			for (Map.Entry<String, M> e : state.entrySet()) {
				System.out.println(
					"ps" + workerId + ":" + e.getKey() + "=" + new Gson().toJson(e.getValue()));
			}
		}
	}

	@Override
	public void flatMap2(Tuple2<Long, String> key, Collector<Tuple3<Long, String, M>> out) {
		if (state.containsKey(key.f1)) {
			out.collect(new Tuple3<>(key.f0, key.f1, state.get(key.f1)));
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//save state, may need modelType
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		//load state
	}
}
