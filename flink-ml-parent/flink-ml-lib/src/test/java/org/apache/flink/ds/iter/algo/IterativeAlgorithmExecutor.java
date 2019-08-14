package org.apache.flink.ds.iter.algo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ds.iter.InferWithPS;
import org.apache.flink.ds.iter.IterativeTrainWithPS;
import org.apache.flink.ds.iter.PsMerger;
import org.apache.flink.ds.iter.StateParamService;
import org.apache.flink.ds.iter.StreamTransformer;
import org.apache.flink.ds.iter.TwoOutputStreamTransformer;
import org.apache.flink.ds.iter.struct.ConvergeSignal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class IterativeAlgorithmExecutor<Data, Model, Update, Loss, LossAcc extends Serializable, Result> implements
	Serializable {
	private IterativeAlgorithm<Data, Model, Update, Loss, LossAcc, Result> algo;
	private String psType;
	private int psParallelism;

	public IterativeAlgorithmExecutor(IterativeAlgorithm<Data, Model, Update, Loss, LossAcc, Result> algo,
		String psType, int psParallelism) {
		this.algo = algo;
		this.psType = psType;
		this.psParallelism = psParallelism;
	}

	public DataStream<Model> fit(DataStream<Model> initialModel, DataStream<Data> data) {
		StateParamService<Data, Model, Update> ps = StateParamService.getPS(psType,
			algo.modelType(), algo.modelKeySelector(),
			algo.updateType(), algo.updateKeySelector(),
			(PsMerger<Model, Update>) (model, update) -> algo.merge(model, update),
			algo.dataType(), algo.dataKeySelector(),
			psParallelism);

		OutputTag<Loss> lossTag = new OutputTag<>("loss", algo.lossType());
		TwoOutputStreamTransformer<Tuple2<Data, Map<String, Model>>, Update, Loss> train =
			(TwoOutputStreamTransformer<Tuple2<Data, Map<String, Model>>, Update, Loss>) in -> {

				SingleOutputStreamOperator<Update> update =
					in.process(new ProcessFunction<Tuple2<Data, Map<String, Model>>, Update>() {
						@Override
						public void processElement(Tuple2<Data, Map<String, Model>> fullData,
							Context ctx,
							Collector<Update> out) throws Exception {
							Tuple2<Iterable<Update>, Loss> result =
								algo.train(fullData.f0, fullData.f1);
							if (result != null) {
								if (result.f0 != null) {
									for (Update u : result.f0) {
										out.collect(u);
									}
								}
								if (result.f1 != null) {
									ctx.output(lossTag, result.f1);
								}
							}
						}
					}).returns(algo.updateType());
				DataStream<Loss> loss = update.getSideOutput(lossTag);
				return new Tuple2<>(update, loss);
			};

		StreamTransformer<Loss, ConvergeSignal> converge =
			(StreamTransformer<Loss, ConvergeSignal>) in ->
				in.flatMap(new FlatMapFunction<Loss, ConvergeSignal>() {
					private LossAcc acc = algo.createLossAcc();

					@Override
					public void flatMap(Loss value, Collector<ConvergeSignal> out) {
						boolean isConverge = algo.isConverge(acc, value);
						if (isConverge) {
							out.collect(ConvergeSignal.create());
						}
					}
				}).setParallelism(1);

		return IterativeTrainWithPS.iterativeTrain(initialModel, data, train, converge, ps);
	}

	public DataStream<Result> transform(DataStream<Model> initialModel,
		DataStream<Model> updateModel,
		DataStream<Data> data) {
		StateParamService<Data, Model, Model> ps = StateParamService.getPS(psType,
			algo.modelType(), algo.modelKeySelector(),
			algo.modelType(), algo.modelKeySelector(),
			(PsMerger<Model, Model>) (model, newModel) -> newModel,
			algo.dataType(), algo.dataKeySelector(),
			psParallelism);

		StreamTransformer<Tuple2<Data, Map<String, Model>>, Result> infer =
			(StreamTransformer<Tuple2<Data, Map<String, Model>>, Result>) in -> in
				.flatMap((FlatMapFunction<Tuple2<Data, Map<String, Model>>, Result>)
					(fullData, out) -> {
						Result result = algo.infer(fullData.f0, fullData.f1);
						if (result != null) {
							out.collect(result);
						}
					}).returns(algo.resultType());

		return InferWithPS.infer(initialModel, updateModel, data, infer, ps);
	}

}
