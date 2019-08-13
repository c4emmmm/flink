package org.apache.flink.ds.iter.test.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ds.iter.PsMerger;
import org.apache.flink.ds.iter.StreamTransformer;
import org.apache.flink.ds.iter.Test;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class SingleModelIterativeAlgorithmEstimator<Model, Update, Data, Result> implements
	Serializable {

	private IterativeAlgorithm<Model, Update, Data, Result> algo;

	public SingleModelIterativeAlgorithmEstimator(IterativeAlgorithm<Model, Update, Data, Result> algo) {
		this.algo = algo;
	}

	public DataStream<Model> fit(DataStream<Model> initialModel, DataStream<Data> data) {
		return Test.mlIterateWithBroadcastPS(
			initialModel,
			new UniqueKeySelector<>(),
			new UniqueKeySelector<>(),
			getPsMerger(),
			data,
			new UniqueKeyArrSelector<>(),
			getTrainer(),
			getConvergeDiscriminator(),
			algo.modelType(),
			algo.updateType(),
			algo.dataType(),
			1
		).map((m) -> m.f1).returns(algo.modelType());
	}

	private PsMerger<Model, Update> getPsMerger() {
		return (PsMerger<Model, Update>) (model, feedback) -> algo.merge(model, feedback);
	}

	private StreamTransformer<Update, Boolean> getConvergeDiscriminator() {
		return (StreamTransformer<Update, Boolean>) (update) -> update.flatMap(
			(FlatMapFunction<Update, Boolean>) (u, out) -> {
				if (algo.isConverge(u)) {
					out.collect(true);
				}
			}).returns(BasicTypeInfo.BOOLEAN_TYPE_INFO).setParallelism(1);
	}

	private StreamTransformer<Tuple2<Data, Map<String, Model>>, Update> getTrainer() {
		return (StreamTransformer<Tuple2<Data, Map<String, Model>>, Update>) (fullData) -> fullData
			.flatMap(
				(FlatMapFunction<Tuple2<Data, Map<String, Model>>, Update>) (d, out) -> {
					Model m = d.f1.get(UniqueKeySelector.UNIQUE_MODEL_KEY);
					if (m == null) {
						return;
					}
					out.collect(algo.train(d.f0, m));
				}).returns(algo.updateType());
	}
}
