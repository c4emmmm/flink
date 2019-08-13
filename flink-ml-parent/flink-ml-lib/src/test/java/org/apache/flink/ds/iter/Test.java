package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ds.iter.broadcast.BroadcastPsCoProcessor;
import org.apache.flink.ds.iter.keyed.AssignDataUUIDAndExtractKeys;
import org.apache.flink.ds.iter.keyed.FlattenDataKey;
import org.apache.flink.ds.iter.keyed.KeyedPsCoProcessor;
import org.apache.flink.ds.iter.keyed.MergeDataFlatMap;
import org.apache.flink.ds.iter.sink.CsvFileOutputFormatFactory;
import org.apache.flink.ds.iter.sink.MultiVersionFileSink;
import org.apache.flink.ds.iter.sink.VersionRecordProcessor;
import org.apache.flink.ds.iter.test.base.SingleModelIterativeAlgorithmEstimator;
import org.apache.flink.ds.iter.test.base.SingleModelIterativeAlgorithmModel;
import org.apache.flink.ds.iter.test.lr.LRInferFlatMap;
import org.apache.flink.ds.iter.test.lr.LRIterativeAlgorithm;
import org.apache.flink.ds.iter.test.lr.LRTrainFlatMap;
import org.apache.flink.ds.iter.test.lr.LrConverge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import com.google.gson.Gson;

import javax.annotation.Nullable;

import java.util.Map;

/**
 *
 */
public class Test {

	@org.junit.Test
	public void testAlgo() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);

		DataStream<double[]> initialModel =
			sEnv.addSource(new SourceFunction<double[]>() {
				@Override
				public void run(SourceContext<double[]> ctx) throws Exception {
					ctx.collect(new double[]{7, 5, 3, 29});
					Thread.sleep(Long.MAX_VALUE);
				}

				@Override
				public void cancel() {

				}
			}).setParallelism(1);

		DataStream<Tuple2<double[], Double>> data = getDataSource(sEnv);

		DataStream<double[]> model =
			new SingleModelIterativeAlgorithmEstimator<>(new LRIterativeAlgorithm())
				.fit(initialModel, data);

		model
			.flatMap((FlatMapFunction<double[], Boolean>) (value, out) -> {
				System.err.println("result=" + new Gson().toJson(value));
			})
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		DataStream<Tuple2<Tuple2<double[], Double>, Double>> result =
			new SingleModelIterativeAlgorithmModel<>(new LRIterativeAlgorithm())
				.transform(initialModel, model, data);

		sEnv.execute();
	}

	@org.junit.Test
	public void testInfer() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);

		DataStream<Tuple2<Integer, Double>> initialModel = getModelSource(sEnv);
		DataStream<Tuple2<double[], Double>> data = getDataSource(sEnv);

		TypeInformation<Tuple2<Integer, Double>> modelType =
			new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO);
		TypeInformation<Tuple2<Integer, Double>> updateType = modelType;
		TypeInformation<Tuple2<double[], Double>> dataType =
			new TupleTypeInfo<>(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO);

		DataStream<Tuple2<Long, Tuple2<Integer, Double>>> model = mlIterateWithBroadcastPS(
			initialModel,
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			(PsMerger<Tuple2<Integer, Double>, Tuple2<Integer, Double>>) (m, f) -> {
				assert (m.f0.equals(f.f0));
				m.f1 += f.f1;
				return m;
			},
			data,
			(KeySelector<Tuple2<double[], Double>, String[]>) value -> {
				String[] keys = new String[value.f0.length + 1];
				for (int i = 0; i < value.f0.length + 1; i++) {
					keys[i] = String.valueOf(i);
				}
				return keys;
			},
			(StreamTransformer<
				Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>>,
				Tuple2<Integer, Double>>) (in) -> in.flatMap(new LRTrainFlatMap()),
			//typically use an aggregator on one iterator and mini-batch with only 1 parallelism to
			// output
			(StreamTransformer<Tuple2<Integer, Double>, Boolean>) u -> u.flatMap(new LrConverge())
				.setParallelism(1),
			modelType,
			updateType,
			dataType,
			3
		);

		DataStream<Tuple2<Integer, Double>> updateStream =
			sEnv.readFile(new RowCsvInputFormat(
					new Path("/Users/hidden/Temp/bucketing_sink/test0/"),
					new TypeInformation<?>[]{BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.DOUBLE_TYPE_INFO}),
				"/Users/hidden/Temp/bucketing_sink/test0/",
				FileProcessingMode.PROCESS_CONTINUOUSLY, 10000,
				new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.DOUBLE_TYPE_INFO))
				.map((MapFunction<Row, Tuple2<Integer, Double>>) value -> new Tuple2<>(
					(Integer) value.getField(0), (Double) value.getField(1))).returns(modelType)
				.setParallelism(3);
		//LRInferFlatMap ignores empty weights, so if initialModel==null, it will ignore all
		// input until update reaches
		DataStream<Tuple3<double[], Double, Double>> result = inferWithKeyedPS(
			initialModel,
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			updateStream,
			//			model.map(vm -> vm.f1).returns(modelType),
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			(PsMerger<Tuple2<Integer, Double>, Tuple2<Integer, Double>>) (m, f) -> f,
			data,
			(KeySelector<Tuple2<double[], Double>, String[]>) value -> {
				String[] keys = new String[value.f0.length + 1];
				for (int i = 0; i < value.f0.length + 1; i++) {
					keys[i] = String.valueOf(i);
				}
				return keys;
			},
			(StreamTransformer<
				Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>>,
				Tuple3<double[], Double, Double>>) (in) -> in.flatMap(new LRInferFlatMap()),
			modelType,
			modelType,
			dataType,
			2
		);

		model.flatMap(
			(FlatMapFunction<Tuple2<Long, Tuple2<Integer, Double>>, Boolean>) (value, out) ->
				System.out.println("model=" + new Gson().toJson(value)))
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		model.addSink(
			new MultiVersionFileSink<>(
				new CsvFileOutputFormatFactory<Tuple2<Integer, Double>>(
					"/Users/hidden/Temp/bucketing_sink/test0") {
					@Override
					public void configure(CsvOutputFormat<Tuple2<Integer, Double>> format) {
					}
				},
				new VersionRecordProcessor<Tuple2<Long, Tuple2<Integer, Double>>, Tuple2<Integer, Double>>() {
					@Override
					public String getVersion(Tuple2<Long, Tuple2<Integer, Double>> record) {
						return String.valueOf(record.f0);
					}

					@Override
					public Tuple2<Integer, Double> getData(Tuple2<Long, Tuple2<Integer, Double>> record) {
						return record.f1;
					}
				})).setParallelism(3);

		result
			.flatMap((FlatMapFunction<Tuple3<double[], Double, Double>, Boolean>) (value, out) -> {
				if (Math.random() > 0.995) {
					System.err.println("result=" + new Gson().toJson(value));
				}
			})
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		updateStream
			.flatMap((FlatMapFunction<Tuple2<Integer, Double>, Boolean>) (value, out) -> {
				System.err.println("read model=" + new Gson().toJson(value));
			})
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		sEnv.execute();
	}

	@org.junit.Test
	public void testPS() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);

		DataStream<Tuple2<Integer, Double>> initialModel = getModelSource(sEnv);
		DataStream<Tuple2<double[], Double>> data = getDataSource(sEnv);

		DataStream<Tuple2<Long, Tuple2<Integer, Double>>> model = mlIterateWithBroadcastPS(
			initialModel,
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			(PsMerger<Tuple2<Integer, Double>, Tuple2<Integer, Double>>) (m, f) -> {
				assert (m.f0.equals(f.f0));
				m.f1 += f.f1;
				return m;
			},
			data,
			(KeySelector<Tuple2<double[], Double>, String[]>) value -> {
				String[] keys = new String[value.f0.length + 1];
				for (int i = 0; i < value.f0.length + 1; i++) {
					keys[i] = String.valueOf(i);
				}
				return keys;
			},
			(StreamTransformer<
				Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>>,
				Tuple2<Integer, Double>>) (in) -> in.flatMap(new LRTrainFlatMap()),
			//typically use an aggregator with only 1 parallelism to output
			//suppose that converge can be judged only with gradient/new model, may need further
			// discussion
			(StreamTransformer<Tuple2<Integer, Double>, Boolean>) u -> u.flatMap(new LrConverge())
				.setParallelism(1),
			new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO),
			new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO),
			new TupleTypeInfo<>(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO),
			3
		);

		model.flatMap(
			(FlatMapFunction<Tuple2<Long, Tuple2<Integer, Double>>, Boolean>) (value, out) ->
				System.out.println("model=" + new Gson().toJson(value)))
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		sEnv.execute();
	}

	public static <M, U, D, R> DataStream<R> inferWithBroadcastPS(
		@Nullable DataStream<M> initialModel,
		@Nullable KeySelector<M, String> modelKeySelector,
		DataStream<U> updateStream,
		KeySelector<U, String> updateKeySelector,
		PsMerger<M, U> merger,
		DataStream<D> sampleData,
		KeySelector<D, String[]> sampleDataKeySelector,
		StreamTransformer<Tuple2<D, Map<String, M>>, R> infer,
		TypeInformation<M> modelType,
		TypeInformation<U> updateType,
		TypeInformation<D> dataType,
		int psParallelism) {

		DataStream<UnifiedModelInput<M, U>> unifiedModelInput =
			unifyInitialModelAndModelUpdate(initialModel, updateStream, modelType, updateType)
				.broadcast();

		DataStream<Tuple2<D, Map<String, M>>> fullData =
			updateOrJoinWithBroadcastPS(
				unifiedModelInput, modelKeySelector, updateKeySelector, merger,
				sampleData, sampleDataKeySelector, modelType, psParallelism);

		return infer.transform(fullData);
	}

	public static <M, U, D, R> DataStream<R> inferWithKeyedPS(
		@Nullable DataStream<M> initialModel,
		@Nullable KeySelector<M, String> modelKeySelector,
		DataStream<U> updateStream,
		KeySelector<U, String> updateKeySelector,
		PsMerger<M, U> merger,
		DataStream<D> sampleData,
		KeySelector<D, String[]> sampleDataKeySelector,
		StreamTransformer<Tuple2<D, Map<String, M>>, R> infer,
		TypeInformation<M> modelType,
		TypeInformation<U> updateType,
		TypeInformation<D> dataType,
		int psParallelism) {

		DataStream<UnifiedModelInput<M, U>> unifiedModelInput =
			unifyInitialModelAndModelUpdate(initialModel, updateStream, modelType, updateType);

		Tuple2<DataStream<Tuple2<D, Map<String, M>>>,
			SingleOutputStreamOperator<Tuple3<Long, String, M>>> fullDataAndPsOperator =
			updateOrJoinWithKeyedPS(
				unifiedModelInput, modelKeySelector, updateKeySelector, merger,
				sampleData, sampleDataKeySelector, modelType, dataType, psParallelism);

		DataStream<Tuple2<D, Map<String, M>>> fullData = fullDataAndPsOperator.f0;

		return infer.transform(fullData);
	}

	public static <M, D, U> DataStream<Tuple2<Long, M>> mlIterateWithBroadcastPS(
		DataStream<M> initialModel,
		KeySelector<M, String> modelKeySelector,
		KeySelector<U, String> updateKeySelector,
		PsMerger<M, U> merger,
		DataStream<D> sampleData,
		KeySelector<D, String[]> sampleDataKeySelector,
		StreamTransformer<Tuple2<D, Map<String, M>>, U> train,
		StreamTransformer<U, Boolean> judgeConverge,
		TypeInformation<M> modelType,
		TypeInformation<U> updateType,
		TypeInformation<D> dataType,
		int psParallelism) {

		//wrapModel
		DataStream<UnifiedModelInput<M, U>> wrapModel =
			initialModel.map(new ModelWrapper<M, U>())
				.returns(UnifiedModelInput.returnType(modelType, updateType));

		//iterate start, model should be broadcast to all ps
		DataStream<UnifiedModelInput<M, U>> unifiedModelInput =
			wrapModel.broadcast().flatMap(new FeedbackHeadFlatMap<>());

		//join data with model, merge update to model on the other side
		SingleOutputStreamOperator<Tuple2<D, Map<String, M>>> fullData =
			updateOrJoinWithBroadcastPS(
				unifiedModelInput, modelKeySelector, updateKeySelector, merger,
				sampleData, sampleDataKeySelector, modelType, psParallelism);

		//train and get the model update, update should be broadcast
		DataStream<U> update = train.transform(fullData);
		DataStream<UnifiedModelInput<M, U>> wrapUpdate =
			update.map(new UpdateWrapper<M, U>())
				.returns(UnifiedModelInput.returnType(modelType, updateType)).broadcast();

		//check if converge, that's enough to send the signal only to the first ps
		DataStream<UnifiedModelInput<M, U>> wrapConvergeSignal =
			judgeConverge.transform(update).map(new FlagToConvergeSignal())
				.map(new ConvergeSignalWrapper<M, U>())
				.returns(UnifiedModelInput.returnType(modelType, updateType));

		//end iteration and feedback
		wrapUpdate.union(wrapConvergeSignal).flatMap(new FeedbackTailFlatMap<>());

		//get and output model
		return fullData.getSideOutput(new OutputTag<>("model",
			new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, modelType)));
	}

	public static <M, U, D> DataStream<Tuple2<Long, M>> mlIterateWithKeyedPS(
		DataStream<M> initialModel,
		KeySelector<M, String> modelKeySelector,
		KeySelector<U, String> updateKeySelector,
		PsMerger<M, U> merger,
		DataStream<D> sampleData,
		KeySelector<D, String[]> sampleDataKeySelector,
		StreamTransformer<Tuple2<D, Map<String, M>>, U> train,
		StreamTransformer<U, Boolean> judgeConverge,
		TypeInformation<M> modelType,
		TypeInformation<U> updateType,
		TypeInformation<D> dataType,
		int psParallelism) {

		//wrapModel
		DataStream<UnifiedModelInput<M, U>> wrapModel = initialModel.map(new ModelWrapper<M, U>())
			.returns(UnifiedModelInput.returnType(modelType, updateType));

		//iterate start, feedback head union initialModel and feedback update/signal
		DataStream<UnifiedModelInput<M, U>> unifiedModelInput =
			wrapModel.flatMap(new FeedbackHeadFlatMap<>());

		Tuple2<DataStream<Tuple2<D, Map<String, M>>>,
			SingleOutputStreamOperator<Tuple3<Long, String, M>>> fullDataAndPsOperator =
			updateOrJoinWithKeyedPS(
				unifiedModelInput, modelKeySelector, updateKeySelector, merger,
				sampleData, sampleDataKeySelector, modelType, dataType, psParallelism);

		//merge flatten data with model into original data
		DataStream<Tuple2<D, Map<String, M>>> fullData = fullDataAndPsOperator.f0;

		//train and get the model update
		DataStream<U> update = train.transform(fullData);
		DataStream<UnifiedModelInput<M, U>> wrapUpdate = update.map(new UpdateWrapper<M, U>())
			.returns(UnifiedModelInput.returnType(modelType, updateType));

		//check if converge, broadcast the signal to all ps
		DataStream<UnifiedModelInput<M, U>> wrapConvergeSignal =
			judgeConverge.transform(update).map(new FlagToConvergeSignal())
				.flatMap(new BroadcastConvergeSignal(psParallelism))
				.map(new ConvergeSignalWrapper<M, U>())
				.returns(UnifiedModelInput.returnType(modelType, updateType));

		//end iteration and feedback
		wrapUpdate.union(wrapConvergeSignal).flatMap(new FeedbackTailFlatMap<>());

		SingleOutputStreamOperator<Tuple3<Long, String, M>> psOperator = fullDataAndPsOperator.f1;
		//get and output model
		return psOperator.getSideOutput(new OutputTag<>("model",
			new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, modelType)));
	}

	public static <M, U, D> SingleOutputStreamOperator<Tuple2<D, Map<String, M>>> updateOrJoinWithBroadcastPS(
		DataStream<UnifiedModelInput<M, U>> unifiedModelInput,
		KeySelector<M, String> modelKeySelector,
		KeySelector<U, String> updateKeySelector,
		PsMerger<M, U> merger,
		DataStream<D> sampleData,
		KeySelector<D, String[]> sampleDataKeySelector,
		TypeInformation<M> modelType,
		int psParallelism) {
		//broadcast here will also broadcast converge signal, currently avoid it
		//in the future, converge signal maybe processed by a specific way, then .broadcast() can
		//be after unifiedModelInput
		return unifiedModelInput.connect(sampleData).process(new BroadcastPsCoProcessor<>(merger,
			modelKeySelector, updateKeySelector, sampleDataKeySelector, modelType))
			.setParallelism(psParallelism);
	}

	public static <M, U, D> Tuple2<DataStream<Tuple2<D, Map<String, M>>>,
		SingleOutputStreamOperator<Tuple3<Long, String, M>>> updateOrJoinWithKeyedPS(
		DataStream<UnifiedModelInput<M, U>> unifiedModelInput,
		KeySelector<M, String> modelKeySelector,
		KeySelector<U, String> updateKeySelector,
		PsMerger<M, U> merger,
		DataStream<D> sampleData,
		KeySelector<D, String[]> sampleDataKeySelector,
		TypeInformation<M> modelType,
		TypeInformation<D> dataType,
		int psParallelism) {
		//flatten data key
		DataStream<Tuple3<Long, String[], D>> sampleDataWithUUID =
			sampleData.flatMap(new AssignDataUUIDAndExtractKeys<>(sampleDataKeySelector));
		DataStream<Tuple2<Long, String>> sampleDataKey =
			sampleDataWithUUID.flatMap(new FlattenDataKey<>());

		//join data with model, merge update to model on the other side
		SingleOutputStreamOperator<Tuple3<Long, String, M>> psOperator =
			unifiedModelInput
				.keyBy(new UnifiedModelInputPartitionKeySelector<>(modelKeySelector,
					updateKeySelector, psParallelism))
				.connect(sampleDataKey.keyBy(new PartitionKeySelector<>(f -> f.f1, psParallelism)))
				.process(new KeyedPsCoProcessor<>(merger,
					modelKeySelector, updateKeySelector,
					modelType)).returns(
				new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
					modelType)).setParallelism(psParallelism);

		DataStream<Tuple3<Long, String, M>> joinResult = psOperator;

		//merge flatten data with model into original data
		DataStream<Tuple2<D, Map<String, M>>> fullData =
			sampleDataWithUUID.keyBy(f -> f.f0).connect(joinResult.keyBy(f -> f.f0))
				.flatMap(new MergeDataFlatMap<>(dataType, modelType));

		//Model can be acquired only in psOperator, so return both fullData and psOperator
		return new Tuple2<>(fullData, psOperator);
	}

	public static <M, U> DataStream<UnifiedModelInput<M, U>> unifyInitialModelAndModelUpdate(
		@Nullable DataStream<M> initialModel,
		DataStream<U> updateStream,
		TypeInformation<M> modelType,
		TypeInformation<U> updateType) {
		DataStream<UnifiedModelInput<M, U>> unifiedInput =
			updateStream.map(new UpdateWrapper<M, U>())
				.returns(UnifiedModelInput.returnType(modelType, updateType));
		if (initialModel != null) {
			DataStream<UnifiedModelInput<M, U>> wrappedModel =
				initialModel.map(new ModelWrapper<M, U>())
					.returns(UnifiedModelInput.returnType(modelType, updateType));
			unifiedInput = unifiedInput.union(wrappedModel);
		}
		return unifiedInput;
	}

	//for test
	public DataStream<Tuple2<double[], Double>> getDataSource(StreamExecutionEnvironment sEnv) {
		return sEnv.addSource(new SourceFunction<Tuple2<double[], Double>>() {
			@Override
			public void run(SourceContext<Tuple2<double[], Double>> ctx) throws Exception {
				while (true) {
					Thread.sleep(10);
					double[] data = new double[]{Math.random(), Math.random(), Math.random()};
					double label = data[0] * 11.1 + data[1] * 17.3 + data[2] * 7.7 + 23;
					ctx.collect(new Tuple2<>(data, label));
				}
			}

			@Override
			public void cancel() {

			}
		});
	}

	//for test
	public DataStream<Tuple2<Integer, Double>> getModelSource(StreamExecutionEnvironment sEnv) {
		return sEnv.addSource(new SourceFunction<Tuple2<Integer, Double>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
				ctx.collect(new Tuple2<>(0, 7.0));
				ctx.collect(new Tuple2<>(1, 5.0));
				ctx.collect(new Tuple2<>(2, 3.0));
				ctx.collect(new Tuple2<>(3, 29.0));
				Thread.sleep(Long.MAX_VALUE);
			}

			@Override
			public void cancel() {

			}
		}).setParallelism(1);
	}
}
