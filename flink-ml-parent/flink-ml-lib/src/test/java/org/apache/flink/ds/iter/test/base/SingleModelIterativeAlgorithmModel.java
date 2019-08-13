package org.apache.flink.ds.iter.test.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.ds.iter.PsMerger;
import org.apache.flink.ds.iter.StreamTransformer;
import org.apache.flink.ds.iter.Test;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class SingleModelIterativeAlgorithmModel<Model, Update, Data, Result> implements
	Serializable {

	private IterativeAlgorithm<Model, Update, Data, Result> algo;

	public SingleModelIterativeAlgorithmModel(IterativeAlgorithm<Model, Update, Data, Result> algo) {
		this.algo = algo;
	}

	public DataStream<Tuple2<Data, Result>> transform(DataStream<Model> initialModel,
		DataStream<Model> newModel, DataStream<Data> data) {
		return Test.inferWithBroadcastPS(
			initialModel,
			new UniqueKeySelector<>(),
			newModel,
			new UniqueKeySelector<>(),
			getPsMerger(),
			data,
			new UniqueKeyArrSelector<>(),
			getPredictor(),
			algo.modelType(),
			algo.modelType(),
			algo.dataType(),
			1
		);
	}

	private PsMerger<Model, Model> getPsMerger() {
		return (PsMerger<Model, Model>) (model, newModel) -> newModel;
	}

	private StreamTransformer<Tuple2<Data, Map<String, Model>>, Tuple2<Data, Result>> getPredictor() {
		return (StreamTransformer<Tuple2<Data, Map<String, Model>>, Tuple2<Data, Result>>) (fullData) -> fullData
			.flatMap(
				(FlatMapFunction<Tuple2<Data, Map<String, Model>>, Tuple2<Data, Result>>) (d, out) -> {
					Model m = d.f1.get(UniqueKeySelector.UNIQUE_MODEL_KEY);
					if (m == null) {
						return;
					}
					out.collect(new Tuple2<>(d.f0, algo.infer(d.f0, m)));
				}).returns(new TupleTypeInfo<>(algo.dataType(), algo.resultType()));
	}
}
