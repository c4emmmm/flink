package org.apache.flink.ds.iter;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @param <M>
 * @param <F>
 */
public class PsCoProcessor<M, F> extends
	RichCoFlatMapFunction<ModelOrFeedback<M, F>, Tuple2<Long, String>, Tuple3<Long, String, M>> {
	private TypeInformation<M> modelType;
	private PsMerger<M, F> merger;
	private ModelOrFeedbackKeySelector<M, F> keySelector;
	private MapState<String, M> state;

	public PsCoProcessor(PsMerger<M, F> merger, ModelOrFeedbackKeySelector<M, F> keySelector,
		TypeInformation<M> modelType) {
		this.merger = merger;
		this.keySelector = keySelector;
		this.modelType = modelType;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.state = getRuntimeContext().getMapState(
			new MapStateDescriptor<>("ps-state", BasicTypeInfo.STRING_TYPE_INFO, modelType));
	}

	@Override
	public void flatMap1(ModelOrFeedback<M, F> value, Collector<Tuple3<Long, String, M>> out)
		throws Exception {
		if (value.isModel) {
			state.put(keySelector.getKey(value), value.model);
		} else {
			state.put(keySelector.getKey(value),
				merger.merge(state.get(keySelector.getKey(value)), value.feedback));
		}
	}

	@Override
	public void flatMap2(Tuple2<Long, String> key, Collector<Tuple3<Long, String, M>> out)
		throws Exception {
		if (state.contains(key.f1)) {
			out.collect(new Tuple3<>(key.f0, key.f1, state.get(key.f1)));
		}
	}
}
