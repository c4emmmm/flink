package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.google.gson.Gson;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 */
public class Test {

	@org.junit.Test
	public void testPS() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);

		DataStream<Tuple2<Integer, Double>> initialModel = getModelSource(sEnv);
		DataStream<Tuple2<double[], Double>> data = getDataSource(sEnv);

		mlIterateWithKeyedPS(
			initialModel,
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			(KeySelector<Tuple2<Integer, Double>, String>) f -> String.valueOf(f.f0),
			data,
			(KeySelector<Tuple2<double[], Double>, String[]>) value -> {
				String[] keys = new String[value.f0.length];
				for (int i = 0; i < value.f0.length; i++) {
					keys[i] = String.valueOf(i);
				}
				return keys;
			},
			(m, f) -> {
				assert (m.f0.equals(f.f0));
				m.f1 += f.f1;
				return m;
			},
			(in) -> in.map(
				(MapFunction<Tuple2<Tuple2<double[], Double>, Map<String, Tuple2<Integer, Double>>>, Tuple3<double[], double[], Double>>) value -> {
					double[] weights = new double[value.f0.f0.length + 1];
					for (Tuple2<Integer, Double> m : value.f1.values()) {
						weights[m.f0] = m.f1;
					}
					System.out.println("model:" + new Gson().toJson(weights));
					return new Tuple3<>(value.f0.f0, weights, value.f0.f1);
				}).flatMap(
				new RichFlatMapFunction<Tuple3<double[], double[], Double>,
					Tuple2<Integer, Double>>() {
					@Override
					public void flatMap(Tuple3<double[], double[], Double> value,
						Collector<Tuple2<Integer, Double>> out) throws Exception {
						RealVector data = new ArrayRealVector(value.f0).append(1);
						RealVector weights = new ArrayRealVector(value.f1);
						double label = value.f2;
						double pred = data.dotProduct(weights);
						double loss = pred - label;
						double[] grad = data.mapMultiply(loss / pred).toArray();
						for (int i = 0; i < grad.length; i++) {
							out.collect(new Tuple2<>(i, grad[i]));
						}
					}
				}),
			in -> (SingleOutputStreamOperator<Tuple2<Integer, Double>>) in,
			new TupleTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO),
			new TupleTypeInfo(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO)
		);

	}

	private <M, D, F, R> void mlIterateWithKeyedPS(
		DataStream<M> initialModel,
		KeySelector<M, String> modelKeySelector,
		KeySelector<F, String> feedbackKeySelector,
		DataStream<D> coData,
		KeySelector<D, String[]> coDataKeySelector,
		PsMerger<M, F> merger,
		StreamTransformer<Tuple2<D, Map<String, M>>, R> compute,
		StreamTransformer<R, F> furtherProcess,
		TypeInformation modelType,
		TypeInformation dataType) {

		DataStream<ModelOrFeedback<M, F>> modelOrFeedback =
			initialModel.flatMap(new FeedbackHeadFlatMap<>());
		SingleOutputStreamOperator<Tuple3<Long, String[], D>> coDataWithUUID =
			coData.flatMap(new DataUUIDAssigner<>(coDataKeySelector));
		DataStream<Tuple2<Long, String>> coDataKey = coDataWithUUID.flatMap(new FlattenDataKey<>());

		SingleOutputStreamOperator<Tuple3<Long, String, M>> joinResult =
			modelOrFeedback
				.keyBy(new ModelOrFeedbackKeySelector<>(modelKeySelector, feedbackKeySelector))
				.connect(coDataKey.keyBy(f -> f.f1))
				.flatMap(new PsCoProcessor<>(merger,
					new ModelOrFeedbackKeySelector<>(modelKeySelector, feedbackKeySelector),
					modelType));

		DataStream<Tuple2<D, Map<String, M>>> fullData =
			coDataWithUUID.keyBy(f -> f.f0).connect(joinResult.keyBy(f -> f.f0))
				.flatMap(new MergeDataFlatMap<>(dataType, modelType));

		SingleOutputStreamOperator<R> feedback = compute.transform(fullData);

		DataStream<M> resultModel = feedback.getSideOutput(new OutputTag<>("model"));
		resultModel.map(v -> {
			System.err.println(v);
			return v;
		});

		furtherProcess.transform(feedback).flatMap(new FeedbackTailFlatMap<>());
	}

	/**
	 *
	 * @param <D>
	 */
	private static class FlattenDataKey<D> extends RichFlatMapFunction<Tuple3<Long, String[], D>,
		Tuple2<Long, String>> {
		@Override
		public void flatMap(Tuple3<Long, String[], D> value,
			Collector<Tuple2<Long, String>> out) throws Exception {
			for (String k : value.f1) {
				out.collect(new Tuple2<>(value.f0, k));
			}
		}
	}

	/**
	 *
	 * @param <M>
	 * @param <F>
	 */
	private static class ModelOrFeedbackKeySelector<M, F> implements
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

	/**
	 *
	 * @param <D>
	 * @param <M>
	 */
	private static class MergeDataFlatMap<D, M> extends
		RichCoFlatMapFunction<Tuple3<Long, String[], D>, Tuple3<Long, String, M>,
			Tuple2<D, Map<String, M>>> {
		private MapState<Long, Tuple3<D, Set<String>, Map<String, M>>> state;
		private TypeInformation<D> dataType;
		private TypeInformation<M> modelType;

		public MergeDataFlatMap(TypeInformation<D> dataType, TypeInformation<M> modelType) {
			this.dataType = dataType;
			this.modelType = modelType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.state = getRuntimeContext().getMapState(
				new MapStateDescriptor<>("ps-state", BasicTypeInfo.LONG_TYPE_INFO,
					new TupleTypeInfo<>(dataType,
						TypeInformation.of(Set.class),
						new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,
							modelType))));
		}

		@Override
		public void flatMap1(Tuple3<Long, String[], D> value,
			Collector<Tuple2<D, Map<String, M>>> out) throws Exception {
			long key = value.f0;
			Tuple3<D, Set<String>, Map<String, M>> message =
				new Tuple3<>(value.f2, new HashSet<>(Arrays.asList(value.f1)), null);
			if (state.contains(key)) {
				Tuple3<D, Set<String>, Map<String, M>> existingMessage = state.get(key);
				for (Map.Entry<String, M> e : existingMessage.f2.entrySet()) {
					process(message, e.getKey(), e.getValue());
				}
			}
			outputOrSave(key, message, out);
		}

		@Override
		public void flatMap2(Tuple3<Long, String, M> value,
			Collector<Tuple2<D, Map<String, M>>> out) throws Exception {
			long key = value.f0;
			Tuple3<D, Set<String>, Map<String, M>> message;
			if (state.contains(key)) {
				message = state.get(key);
			} else {
				message = new Tuple3<>(null, new HashSet<>(), new HashMap<>());
			}
			if (message.f0 == null) {
				message.f2.put(value.f1, value.f2);
				state.put(key, message);
			} else {
				process(message, value.f1, value.f2);
				outputOrSave(key, message, out);
			}
		}

		private void process(Tuple3<D, Set<String>, Map<String, M>> message, String key, M value) {
			message.f1.remove(key);
			message.f2.put(key, value);
		}

		private void outputOrSave(long key, Tuple3<D, Set<String>, Map<String, M>> message,
			Collector<Tuple2<D, Map<String, M>>> out) throws Exception {
			if (message.f1.isEmpty()) {
				state.remove(key);
				out.collect(new Tuple2<>(message.f0, message.f2));
			} else {
				state.put(key, message);
			}
		}

	}

	/**
	 *
	 * @param <D>
	 */
	public static class DataUUIDAssigner<D> extends RichFlatMapFunction<D, Tuple3<Long, String[], D>> {
		KeySelector<D, String[]> keyExtractor;

		public DataUUIDAssigner(KeySelector<D, String[]> keyExtractor) {
			this.keyExtractor = keyExtractor;
		}

		@Override
		public void flatMap(D value, Collector<Tuple3<Long, String[], D>> out) throws Exception {
			long id = nextId();
			out.collect(new Tuple3<>(id, keyExtractor.getKey(value), value));
		}

		private long nextId() {
			return System.nanoTime() * 1000 + (long) Math.floor(Math.random() * 1000);
		}
	}

	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 */
	private interface StreamTransformer<IN, OUT> {
		SingleOutputStreamOperator<OUT> transform(DataStream<IN> in);
	}

	/**
	 *
	 * @param <M>
	 * @param <F>
	 */
	private interface PsMerger<M, F> {
		M merge(M model, F feedback);
	}

	/**
	 *
	 * @param <M>
	 * @param <F>
	 */
	private static class PsCoProcessor<M, F> extends
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
			out.collect(new Tuple3<>(key.f0, key.f1, state.get(key.f1)));
		}
	}

	/**
	 *
	 * @param <F>
	 */
	public static class FeedbackTailFlatMap<F>
		extends RichFlatMapFunction<F, F> {

		@Override
		public void flatMap(F value, Collector<F> out) throws Exception {
			FeedbackHeadFlatMap.queue.offer(value);
		}
	}

	/**
	 *
	 * @param <M>
	 * @param <F>
	 */
	public static class FeedbackHeadFlatMap<M, F>
		extends RichFlatMapFunction<M, ModelOrFeedback<M, F>> {
		public static LinkedBlockingQueue queue = new LinkedBlockingQueue();
		public Boolean running = false;
		public CollectThread thread;
		public final Object lock = new Object();

		@Override
		public void flatMap(M value, Collector<ModelOrFeedback<M, F>> out) throws Exception {
			if (!running) {
				synchronized (lock) {
					if (!running) {
						thread = new CollectThread(out);
						thread.run();
						running = true;
					}
				}
			}
			out.collect(new ModelOrFeedback<>(false, value, null));
		}

		/**
		 *
		 */
		public class CollectThread extends Thread {
			Collector<ModelOrFeedback<M, F>> out;

			public CollectThread(Collector<ModelOrFeedback<M, F>> out) {
				this.out = out;
			}

			@Override
			public void run() {
				try {
					F feedback = (F) queue.take();
					out.collect(new ModelOrFeedback<>(false, null, feedback));
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	/**
	 *
	 * @param <M>
	 * @param <F>
	 */
	private static class ModelOrFeedback<M, F> {
		boolean isModel; //or feedback
		M model;
		F feedback;

		public ModelOrFeedback(boolean isModel, M model, F feedback) {
			this.isModel = isModel;
			this.model = model;
			this.feedback = feedback;
		}

	}

	private DataStream<Tuple2<double[], Double>> getDataSource(StreamExecutionEnvironment sEnv) {
		return sEnv.addSource(new SourceFunction<Tuple2<double[], Double>>() {
			@Override
			public void run(SourceContext<Tuple2<double[], Double>> ctx) throws Exception {
				while (true) {
					Thread.sleep(10);
					double[] data = new double[]{
						Math.floor(Math.random() * 100) * 1.0,
						Math.floor(Math.random() * 100) * 1.0,
						Math.floor(Math.random() * 100) * 1.0
					};
					double label = data[0] * 11.1 + data[1] * 17.3 + data[3] * 7.7 + 23;
					ctx.collect(new Tuple2<>(data, label));
				}
			}

			@Override
			public void cancel() {

			}
		});
	}

	private DataStream<Tuple2<Integer, Double>> getModelSource(StreamExecutionEnvironment sEnv) {
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
		});
	}

	//	@org.junit.Test
	//	public void testLR() throws Exception {
	//		int parallelism = 3;
	//		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);
	//
	//		DataStream<Data> model = sEnv.addSource(new SourceFunction<Data>() {
	//			@Override
	//			public void run(SourceContext<Data> ctx) throws Exception {
	//				while (true) {
	//					Thread.sleep(60000);
	//					Data c00 = new Data();
	//					c00.data = new double[]{0, 0};
	//					c00.key = "00";
	//					ctx.collect(c00);
	//					Data c01 = new Data();
	//					c01.data = new double[]{0, 1};
	//					c01.key = "01";
	//					ctx.collect(c01);
	//					Data c10 = new Data();
	//					c10.data = new double[]{1, 0};
	//					c10.key = "10";
	//					ctx.collect(c10);
	//					Data c11 = new Data();
	//					c11.data = new double[]{1, 1};
	//					c11.key = "11";
	//					ctx.collect(c11);
	//					Data eob = new Data();
	//					eob.isEob = true;
	//					eob.isData = false;
	//					ctx.collect(eob);
	//				}
	//			}
	//
	//			@Override
	//			public void cancel() {
	//
	//			}
	//		});
	//		DataStream<Data> data = sEnv.addSource(new SourceFunction<Data>() {
	//			String[] keys = new String[]{"c00", "c01", "c10", "c11"};
	//			double[][] cs =
	//				new double[][]{new double[]{0, 0}, new double[]{0, 1}, new double[]{1, 0},
	//					new double[]{1, 1}};
	//
	//			@Override
	//			public void run(SourceContext<Data> ctx) throws Exception {
	//				while (true) {
	//					Thread.sleep(10);
	//					Data d = new Data();
	//					int idx = (int) (Math.random() * 4);
	//					d.data = cs[idx];
	//					d.key = keys[idx];
	//					d.data = new double[]{d.data[0] + Math.random() * 0.6 - 0.3,
	//						d.data[1] + Math.random() * 0.6 - 0.3};
	//					ctx.collect(d);
	//				}
	//			}
	//
	//			@Override
	//			public void cancel() {
	//
	//			}
	//		});
	//
	//		SplitStream<Data> ss = model.broadcast().flatMap(new RichFlatMapFunction<Data, Data>() {
	//			List<Data> queue;
	//			int workerId;
	//
	//			@Override
	//			public void open(Configuration parameters) throws Exception {
	//				super.open(parameters);
	//				queue = new ArrayList<>();
	//				workerId = getRuntimeContext().getIndexOfThisSubtask();
	//			}
	//
	//			@Override
	//			public void flatMap(Data value, Collector<Data> out) throws Exception {
	//				if (value.isData) {
	//					queue.add(value);
	//				}
	//				if (value.isEob) {
	//					while (true) {
	//						boolean isConverge = value.isConverge;
	//						for (Data d : queue) {
	//							d.isOutput = isConverge;
	//							out.collect(d);
	//						}
	//						queue.clear();
	//						if (!isConverge) {
	//							out.collect(value);
	//						} else {
	//							break;
	//						}
	//						while (true) {
	//							Data d = HeadTailQueue.getBroadcastQueue(workerId).take();
	//							if (d.isData) {
	//								queue.add(d);
	//							}
	//							if (d.isEob) {
	//								value = d;
	//								break;
	//							}
	//						}
	//					}
	//				}
	//			}
	//		}).split(
	//			(OutputSelector<Data>) (value) -> Collections
	//				.singletonList(value.isOutput ? "output" : "iterate"));
	//
	//		ss.select("output").writeUsingOutputFormat(new PrintingOutputFormat<>()).setParallelism(1);
	//
	//		DataStream<Data> iter = ss.select("iterate");
	//		DataStream<Data> delta =
	//			iter.connect(data).flatMap(new RichCoFlatMapFunction<Data, Data, Data>() {
	//				int workerId = -1;
	//
	//				int iterCnt = 0;
	//				List<Data> model = new LinkedList<>();
	//				boolean modelReady = false;
	//
	//				int leastDataCount = 100;
	//				List<Data> data = new LinkedList<>();
	//				boolean dataReady = false;
	//
	//				@Override
	//				public void open(Configuration parameters) throws Exception {
	//					super.open(parameters);
	//					workerId = getRuntimeContext().getIndexOfThisSubtask();
	//				}
	//
	//				@Override
	//				public void flatMap1(Data value, Collector<Data> out) throws Exception {
	//					if (!value.isEob) {
	//						model.add(value);
	//					} else {
	//						iterCnt = value.iterCount;
	//						modelReady = true;
	//					}
	//					if (dataReady && modelReady) {
	//						compute(out, model, data, iterCnt);
	//						modelReady = false;
	//						dataReady = false;
	//						iterCnt = 0;
	//					}
	//				}
	//
	//				@Override
	//				public void flatMap2(Data value, Collector<Data> out) throws Exception {
	//					data.add(value);
	//					dataReady = data.size() >= leastDataCount;
	//					if (dataReady && modelReady) {
	//						compute(out, model, data, iterCnt);
	//						modelReady = false;
	//						dataReady = false;
	//					}
	//				}
	//
	//				private void compute(
	//					Collector<Data> out,
	//					List<Data> model,
	//					List<Data> data,
	//					int iterCnt) {
	//					Map<String, double[]> centroids = buildModel(model);
	//					Map<String, List<double[]>> newCentroidsCache = new HashMap<>();
	//					for (Data d : data) {
	//						String nearest = nearest(d, centroids);
	//						updateCache(nearest, newCentroidsCache, d.data);
	//					}
	//					Map<String, double[]> newCentroids = toCentroids(newCentroidsCache);
	//					boolean isConverge = iterCnt > 3 && isConverge(centroids, newCentroids);
	//					collectModel(out, newCentroids, isConverge, iterCnt);
	//
	//					model.clear();
	//					data.clear();
	//					//					dataReady = data.size() >= leastDataCount;
	//				}
	//
	//				private void collectModel(
	//					Collector<Data> out,
	//					Map<String, double[]> newCentroids,
	//					boolean isConverge,
	//					int iterCnt) {
	//					System.err.println(workerId + " ----------------------------------");
	//					System.err.println(workerId + " collect model iter:" + iterCnt);
	//					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
	//						Data data = new Data();
	//						data.key = e.getKey();
	//						data.data = e.getValue();
	//						out.collect(data);
	//						System.err.println(
	//							workerId + " m:" + data.key + ", " + Arrays.toString(data.data));
	//					}
	//					System.err.println(workerId + " ----------------------------------");
	//
	//					Data eob = new Data();
	//					eob.isEob = true;
	//					eob.isData = false;
	//					eob.isConverge = isConverge;
	//					eob.iterCount = iterCnt + 1;
	//					out.collect(eob);
	//				}
	//
	//				private boolean isConverge(
	//					Map<String, double[]> centroids,
	//					Map<String, double[]> newCentroids) {
	//					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
	//						double[] newC = e.getValue();
	//						double[] c = centroids.get(e.getKey());
	//						double dist = new ArrayRealVector(newC).getDistance(new ArrayRealVector(c));
	//						System.out.print(dist + ",");
	//						if (dist > 0.1) {
	//							System.out.println();
	//							return false;
	//						}
	//					}
	//					System.out.println();
	//					return true;
	//				}
	//
	//				private Map<String, double[]> toCentroids(
	//					Map<String, List<double[]>> newCentroidsCache) {
	//					Map<String, double[]> newCentroids = new HashMap<>();
	//					for (Map.Entry<String, List<double[]>> e : newCentroidsCache.entrySet()) {
	//						ArrayRealVector sum = null;
	//						for (double[] point : e.getValue()) {
	//							sum = sum == null ? new ArrayRealVector(point) :
	//								sum.add(new ArrayRealVector(point));
	//						}
	//
	//						newCentroids.put(e.getKey(), sum.mapDivide(e.getValue().size()).toArray());
	//					}
	//
	//					return newCentroids;
	//				}
	//
	//				private void updateCache(
	//					String nearest,
	//					Map<String, List<double[]>> newCentroidsCache, double[] data) {
	//					newCentroidsCache.computeIfAbsent(nearest, k -> new ArrayList<>()).add(data);
	//
	//				}
	//
	//				private String nearest(Data d, Map<String, double[]> centroids) {
	//					double[] point = d.data;
	//					String nearest = null;
	//					double distance = Double.MAX_VALUE;
	//					for (Map.Entry<String, double[]> e : centroids.entrySet()) {
	//						double dist =
	//							new ArrayRealVector(e.getValue())
	//								.getDistance(new ArrayRealVector(point));
	//						if (dist < distance) {
	//							distance = dist;
	//							nearest = e.getKey();
	//						}
	//					}
	//					return nearest;
	//				}
	//
	//				private Map<String, double[]> buildModel(List<Data> model) {
	//					Map<String, double[]> map = new HashMap<>();
	//					for (Data d : model) {
	//						map.put(d.key, d.data);
	//					}
	//					return map;
	//				}
	//			});
	//
	//		delta.keyBy(d -> "1").flatMap(new FlatMapFunction<Data, Data>() {
	//			int cnt = 0;
	//			Map<String, List<double[]>> modelCache = new HashMap<>();
	//			boolean isConverge = true;
	//
	//			@Override
	//			public void flatMap(Data value, Collector<Data> out) throws Exception {
	//				if (value.isData) {
	//					List<double[]> c =
	//						modelCache.computeIfAbsent(value.key, k -> new ArrayList<>());
	//					c.add(value.data);
	//				}
	//				if (value.isEob) {
	//					isConverge &= value.isConverge;
	//					cnt += 1;
	//				}
	//				if (cnt == parallelism) {
	//					collectModel(out, toCentroids(modelCache), isConverge, value.iterCount);
	//					cnt = 0;
	//					modelCache = new HashMap<>();
	//					isConverge = true;
	//				}
	//			}
	//
	//			private Map<String, double[]> toCentroids(
	//				Map<String, List<double[]>> newCentroidsCache) {
	//				Map<String, double[]> newCentroids = new HashMap<>();
	//				for (Map.Entry<String, List<double[]>> e : newCentroidsCache.entrySet()) {
	//					ArrayRealVector sum = null;
	//					for (double[] point : e.getValue()) {
	//						sum = sum == null ? new ArrayRealVector(point) :
	//							sum.add(new ArrayRealVector(point));
	//					}
	//
	//					newCentroids.put(e.getKey(), sum.mapDivide(e.getValue().size()).toArray());
	//				}
	//
	//				return newCentroids;
	//			}
	//
	//			private void collectModel(
	//				Collector<Data> out,
	//				Map<String, double[]> newCentroids,
	//				boolean isConverge,
	//				int iterCnt) {
	//				System.err.println("----------------------------------");
	//				System.err.println("reduce collect model iter:" + iterCnt);
	//				for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
	//					Data data = new Data();
	//					data.key = e.getKey();
	//					data.data = e.getValue();
	//					//					out.collect(data);
	//					HeadTailQueue.broadcastOffer(parallelism, data);
	//					System.err.println("m:" + data.key + ", " + Arrays.toString(data.data));
	//				}
	//				System.err.println("----------------------------------");
	//
	//				Data eob = new Data();
	//				eob.isEob = true;
	//				eob.isData = false;
	//				eob.isConverge = isConverge;
	//				eob.iterCount = iterCnt + 1;
	//				//				out.collect(eob);
	//				HeadTailQueue.broadcastOffer(parallelism, eob);
	//			}
	//		});
	//
	//		sEnv.execute();
	//	}
	//
	//	@org.junit.Test
	//	public void test() throws Exception {
	//		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
	//
	//		DataStream<Data> model = sEnv.addSource(new SourceFunction<Data>() {
	//			@Override
	//			public void run(SourceContext<Data> ctx) throws Exception {
	//				while (true) {
	//					Thread.sleep(60000);
	//					Data c00 = new Data();
	//					c00.data = new double[]{0, 0};
	//					c00.key = "00";
	//					ctx.collect(c00);
	//					Data c01 = new Data();
	//					c01.data = new double[]{0, 1};
	//					c01.key = "01";
	//					ctx.collect(c01);
	//					Data c10 = new Data();
	//					c10.data = new double[]{1, 0};
	//					c10.key = "10";
	//					ctx.collect(c10);
	//					Data c11 = new Data();
	//					c11.data = new double[]{1, 1};
	//					c11.key = "11";
	//					ctx.collect(c11);
	//					Data eob = new Data();
	//					eob.isEob = true;
	//					eob.isData = false;
	//					ctx.collect(eob);
	//				}
	//			}
	//
	//			@Override
	//			public void cancel() {
	//
	//			}
	//		});
	//		DataStream<Data> data = sEnv.addSource(new SourceFunction<Data>() {
	//			String[] keys = new String[]{"c00", "c01", "c10", "c11"};
	//			double[][] cs =
	//				new double[][]{new double[]{0, 0}, new double[]{0, 1}, new double[]{1, 0},
	//					new double[]{1, 1}};
	//
	//			@Override
	//			public void run(SourceContext<Data> ctx) throws Exception {
	//				while (true) {
	//					Thread.sleep(10);
	//					Data d = new Data();
	//					int idx = (int) (Math.random() * 4);
	//					d.data = cs[idx];
	//					d.key = keys[idx];
	//					d.data = new double[]{d.data[0] + Math.random() * 0.6 - 0.3,
	//						d.data[1] + Math.random() * 0.6 - 0.3};
	//					ctx.collect(d);
	//				}
	//			}
	//
	//			@Override
	//			public void cancel() {
	//
	//			}
	//		});
	//
	//		SplitStream<Data> ss = model.broadcast().flatMap(new RichFlatMapFunction<Data, Data>() {
	//			List<Data> queue;
	//
	//			@Override
	//			public void open(Configuration parameters) throws Exception {
	//				super.open(parameters);
	//				queue = new ArrayList<>();
	//			}
	//
	//			@Override
	//			public void flatMap(Data value, Collector<Data> out) throws Exception {
	//				if (value.isData) {
	//					queue.add(value);
	//				}
	//				if (value.isEob) {
	//					while (true) {
	//						boolean isConverge = value.isConverge;
	//						for (Data d : queue) {
	//							d.isOutput = isConverge;
	//							out.collect(d);
	//						}
	//						queue.clear();
	//						if (!isConverge) {
	//							out.collect(value);
	//						} else {
	//							break;
	//						}
	//						while (true) {
	//							Data d = HeadTailQueue.queue.take();
	//							if (d.isData) {
	//								queue.add(d);
	//							}
	//							if (d.isEob) {
	//								value = d;
	//								break;
	//							}
	//						}
	//					}
	//				}
	//			}
	//		}).split(
	//			(OutputSelector<Data>) (value) -> Collections
	//				.singletonList(value.isOutput ? "output" : "iterate"));
	//
	//		ss.select("output").writeUsingOutputFormat(new PrintingOutputFormat<>());
	//
	//		DataStream<Data> iter = ss.select("iterate");
	//		DataStream<Data> delta =
	//			iter.connect(data).flatMap(new CoFlatMapFunction<Data, Data, Data>() {
	//				int iterCnt = 0;
	//				List<Data> model = new LinkedList<>();
	//				boolean modelReady = false;
	//
	//				int leastDataCount = 100;
	//				List<Data> data = new LinkedList<>();
	//				boolean dataReady = false;
	//
	//				@Override
	//				public void flatMap1(Data value, Collector<Data> out) throws Exception {
	//					if (!value.isEob) {
	//						model.add(value);
	//					} else {
	//						iterCnt = value.iterCount;
	//						modelReady = true;
	//					}
	//					if (dataReady && modelReady) {
	//						compute(out, model, data, iterCnt);
	//						modelReady = false;
	//						dataReady = false;
	//						iterCnt = 0;
	//					}
	//				}
	//
	//				@Override
	//				public void flatMap2(Data value, Collector<Data> out) throws Exception {
	//					data.add(value);
	//					dataReady = data.size() >= leastDataCount;
	//					if (dataReady && modelReady) {
	//						compute(out, model, data, iterCnt);
	//						modelReady = false;
	//						dataReady = false;
	//					}
	//				}
	//
	//				private void compute(
	//					Collector<Data> out,
	//					List<Data> model,
	//					List<Data> data,
	//					int iterCnt) {
	//					Map<String, double[]> centroids = buildModel(model);
	//					Map<String, List<double[]>> newCentroidsCache = new HashMap<>();
	//					for (Data d : data) {
	//						String nearest = nearest(d, centroids);
	//						updateCache(nearest, newCentroidsCache, d.data);
	//					}
	//					Map<String, double[]> newCentroids = toCentroids(newCentroidsCache);
	//					boolean isConverge = iterCnt > 3 && isConverge(centroids, newCentroids);
	//					collectModel(out, newCentroids, isConverge, iterCnt);
	//
	//					model.clear();
	//					data.clear();
	//					//					dataReady = data.size() >= leastDataCount;
	//				}
	//
	//				private void collectModel(
	//					Collector<Data> out,
	//					Map<String, double[]> newCentroids,
	//					boolean isConverge,
	//					int iterCnt) {
	//					System.err.println("----------------------------------");
	//					System.err.println("collect model iter:" + iterCnt);
	//					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
	//						Data data = new Data();
	//						data.key = e.getKey();
	//						data.data = e.getValue();
	//						//						out.collect(data);
	//						HeadTailQueue.queue.offer(data);
	//						System.err.println("m:" + data.key + ", " + Arrays.toString(data.data));
	//					}
	//					System.err.println("----------------------------------");
	//
	//					Data eob = new Data();
	//					eob.isEob = true;
	//					eob.isData = false;
	//					eob.isConverge = isConverge;
	//					eob.iterCount = iterCnt + 1;
	//					//					out.collect(eob);
	//					HeadTailQueue.queue.offer(eob);
	//				}
	//
	//				private boolean isConverge(
	//					Map<String, double[]> centroids,
	//					Map<String, double[]> newCentroids) {
	//					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
	//						double[] newC = e.getValue();
	//						double[] c = centroids.get(e.getKey());
	//						double dist = new ArrayRealVector(newC).getDistance(new ArrayRealVector(c));
	//						System.out.print(dist + ",");
	//						if (dist > 0.1) {
	//							System.out.println();
	//							return false;
	//						}
	//					}
	//					System.out.println();
	//					return true;
	//				}
	//
	//				private Map<String, double[]> toCentroids(
	//					Map<String, List<double[]>> newCentroidsCache) {
	//					Map<String, double[]> newCentroids = new HashMap<>();
	//					for (Map.Entry<String, List<double[]>> e : newCentroidsCache.entrySet()) {
	//						ArrayRealVector sum = null;
	//						for (double[] point : e.getValue()) {
	//							sum = sum == null ? new ArrayRealVector(point) :
	//								sum.add(new ArrayRealVector(point));
	//						}
	//
	//						newCentroids.put(e.getKey(), sum.mapDivide(e.getValue().size()).toArray());
	//					}
	//
	//					return newCentroids;
	//				}
	//
	//				private void updateCache(
	//					String nearest,
	//					Map<String, List<double[]>> newCentroidsCache, double[] data) {
	//					newCentroidsCache.computeIfAbsent(nearest, k -> new ArrayList<>()).add(data);
	//
	//				}
	//
	//				private String nearest(Data d, Map<String, double[]> centroids) {
	//					double[] point = d.data;
	//					String nearest = null;
	//					double distance = Double.MAX_VALUE;
	//					for (Map.Entry<String, double[]> e : centroids.entrySet()) {
	//						double dist =
	//							new ArrayRealVector(e.getValue())
	//								.getDistance(new ArrayRealVector(point));
	//						if (dist < distance) {
	//							distance = dist;
	//							nearest = e.getKey();
	//						}
	//					}
	//					return nearest;
	//				}
	//
	//				private Map<String, double[]> buildModel(List<Data> model) {
	//					Map<String, double[]> map = new HashMap<>();
	//					for (Data d : model) {
	//						map.put(d.key, d.data);
	//					}
	//					return map;
	//				}
	//			});
	//
	//		sEnv.execute();
	//	}
	//
	//	/**
	//	 *
	//	 */
	//	public static class Data {
	//		public boolean isEob = false;
	//		public boolean isValue = true; //or delta
	//		public boolean isOutput = false;
	//		public boolean isConverge = false;
	//		public boolean isData = true;
	//		public int iterCount = 0;
	//		public String key;
	//		public double[] data;
	//	}
	//
	//	/**
	//	 *
	//	 */
	//	public static class HeadTailQueue {
	//		public static LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<>();
	//		public static Map<Integer, LinkedBlockingQueue<Data>> broadcastQueue =
	//			new HashMap<>();
	//		private static final Object lock = new Object();
	//
	//		public static LinkedBlockingQueue<Data> getBroadcastQueue(int key) {
	//			synchronized (lock) {
	//				return broadcastQueue.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
	//			}
	//		}
	//
	//		public static void broadcastOffer(int parallelism, Data data) {
	//			synchronized (lock) {
	//				for (int i = 0; i < parallelism; i++) {
	//					broadcastQueue.computeIfAbsent(i, k -> new LinkedBlockingQueue<>()).offer(data);
	//				}
	//			}
	//		}
	//	}
}
