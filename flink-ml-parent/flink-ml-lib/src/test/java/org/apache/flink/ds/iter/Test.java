package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import org.apache.commons.math3.linear.ArrayRealVector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 */
public class Test {

	@org.junit.Test
	public void testLR() throws Exception {
		int parallelism = 3;
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);

		DataStream<Data> model = sEnv.addSource(new SourceFunction<Data>() {
			@Override
			public void run(SourceContext<Data> ctx) throws Exception {
				while (true) {
					Thread.sleep(60000);
					Data c00 = new Data();
					c00.data = new double[]{0, 0};
					c00.key = "00";
					ctx.collect(c00);
					Data c01 = new Data();
					c01.data = new double[]{0, 1};
					c01.key = "01";
					ctx.collect(c01);
					Data c10 = new Data();
					c10.data = new double[]{1, 0};
					c10.key = "10";
					ctx.collect(c10);
					Data c11 = new Data();
					c11.data = new double[]{1, 1};
					c11.key = "11";
					ctx.collect(c11);
					Data eob = new Data();
					eob.isEob = true;
					eob.isData = false;
					ctx.collect(eob);
				}
			}

			@Override
			public void cancel() {

			}
		});
		DataStream<Data> data = sEnv.addSource(new SourceFunction<Data>() {
			String[] keys = new String[]{"c00", "c01", "c10", "c11"};
			double[][] cs =
				new double[][]{new double[]{0, 0}, new double[]{0, 1}, new double[]{1, 0},
					new double[]{1, 1}};

			@Override
			public void run(SourceContext<Data> ctx) throws Exception {
				while (true) {
					Thread.sleep(10);
					Data d = new Data();
					int idx = (int) (Math.random() * 4);
					d.data = cs[idx];
					d.key = keys[idx];
					d.data = new double[]{d.data[0] + Math.random() * 0.6 - 0.3,
						d.data[1] + Math.random() * 0.6 - 0.3};
					ctx.collect(d);
				}
			}

			@Override
			public void cancel() {

			}
		});

		SplitStream<Data> ss = model.broadcast().flatMap(new RichFlatMapFunction<Data, Data>() {
			List<Data> queue;
			int workerId;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				queue = new ArrayList<>();
				workerId = getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public void flatMap(Data value, Collector<Data> out) throws Exception {
				if (value.isData) {
					queue.add(value);
				}
				if (value.isEob) {
					while (true) {
						boolean isConverge = value.isConverge;
						for (Data d : queue) {
							d.isOutput = isConverge;
							out.collect(d);
						}
						queue.clear();
						if (!isConverge) {
							out.collect(value);
						} else {
							break;
						}
						while (true) {
							Data d = HeadTailQueue.getBroadcastQueue(workerId).take();
							if (d.isData) {
								queue.add(d);
							}
							if (d.isEob) {
								value = d;
								break;
							}
						}
					}
				}
			}
		}).split(
			(OutputSelector<Data>) (value) -> Collections
				.singletonList(value.isOutput ? "output" : "iterate"));

		ss.select("output").writeUsingOutputFormat(new PrintingOutputFormat<>()).setParallelism(1);

		DataStream<Data> iter = ss.select("iterate");
		DataStream<Data> delta =
			iter.connect(data).flatMap(new RichCoFlatMapFunction<Data, Data, Data>() {
				int workerId = -1;

				int iterCnt = 0;
				List<Data> model = new LinkedList<>();
				boolean modelReady = false;

				int leastDataCount = 100;
				List<Data> data = new LinkedList<>();
				boolean dataReady = false;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					workerId = getRuntimeContext().getIndexOfThisSubtask();
				}

				@Override
				public void flatMap1(Data value, Collector<Data> out) throws Exception {
					if (!value.isEob) {
						model.add(value);
					} else {
						iterCnt = value.iterCount;
						modelReady = true;
					}
					if (dataReady && modelReady) {
						compute(out, model, data, iterCnt);
						modelReady = false;
						dataReady = false;
						iterCnt = 0;
					}
				}

				@Override
				public void flatMap2(Data value, Collector<Data> out) throws Exception {
					data.add(value);
					dataReady = data.size() >= leastDataCount;
					if (dataReady && modelReady) {
						compute(out, model, data, iterCnt);
						modelReady = false;
						dataReady = false;
					}
				}

				private void compute(
					Collector<Data> out,
					List<Data> model,
					List<Data> data,
					int iterCnt) {
					Map<String, double[]> centroids = buildModel(model);
					Map<String, List<double[]>> newCentroidsCache = new HashMap<>();
					for (Data d : data) {
						String nearest = nearest(d, centroids);
						updateCache(nearest, newCentroidsCache, d.data);
					}
					Map<String, double[]> newCentroids = toCentroids(newCentroidsCache);
					boolean isConverge = iterCnt > 3 && isConverge(centroids, newCentroids);
					collectModel(out, newCentroids, isConverge, iterCnt);

					model.clear();
					data.clear();
					//					dataReady = data.size() >= leastDataCount;
				}

				private void collectModel(
					Collector<Data> out,
					Map<String, double[]> newCentroids,
					boolean isConverge,
					int iterCnt) {
					System.err.println(workerId + " ----------------------------------");
					System.err.println(workerId + " collect model iter:" + iterCnt);
					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
						Data data = new Data();
						data.key = e.getKey();
						data.data = e.getValue();
						out.collect(data);
						System.err.println(
							workerId + " m:" + data.key + ", " + Arrays.toString(data.data));
					}
					System.err.println(workerId + " ----------------------------------");

					Data eob = new Data();
					eob.isEob = true;
					eob.isData = false;
					eob.isConverge = isConverge;
					eob.iterCount = iterCnt + 1;
					out.collect(eob);
				}

				private boolean isConverge(
					Map<String, double[]> centroids,
					Map<String, double[]> newCentroids) {
					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
						double[] newC = e.getValue();
						double[] c = centroids.get(e.getKey());
						double dist = new ArrayRealVector(newC).getDistance(new ArrayRealVector(c));
						System.out.print(dist + ",");
						if (dist > 0.1) {
							System.out.println();
							return false;
						}
					}
					System.out.println();
					return true;
				}

				private Map<String, double[]> toCentroids(
					Map<String, List<double[]>> newCentroidsCache) {
					Map<String, double[]> newCentroids = new HashMap<>();
					for (Map.Entry<String, List<double[]>> e : newCentroidsCache.entrySet()) {
						ArrayRealVector sum = null;
						for (double[] point : e.getValue()) {
							sum = sum == null ? new ArrayRealVector(point) :
								sum.add(new ArrayRealVector(point));
						}

						newCentroids.put(e.getKey(), sum.mapDivide(e.getValue().size()).toArray());
					}

					return newCentroids;
				}

				private void updateCache(
					String nearest,
					Map<String, List<double[]>> newCentroidsCache, double[] data) {
					newCentroidsCache.computeIfAbsent(nearest, k -> new ArrayList<>()).add(data);

				}

				private String nearest(Data d, Map<String, double[]> centroids) {
					double[] point = d.data;
					String nearest = null;
					double distance = Double.MAX_VALUE;
					for (Map.Entry<String, double[]> e : centroids.entrySet()) {
						double dist =
							new ArrayRealVector(e.getValue())
								.getDistance(new ArrayRealVector(point));
						if (dist < distance) {
							distance = dist;
							nearest = e.getKey();
						}
					}
					return nearest;
				}

				private Map<String, double[]> buildModel(List<Data> model) {
					Map<String, double[]> map = new HashMap<>();
					for (Data d : model) {
						map.put(d.key, d.data);
					}
					return map;
				}
			});

		delta.keyBy(d -> "1").flatMap(new FlatMapFunction<Data, Data>() {
			int cnt = 0;
			Map<String, List<double[]>> modelCache = new HashMap<>();
			boolean isConverge = true;

			@Override
			public void flatMap(Data value, Collector<Data> out) throws Exception {
				if (value.isData) {
					List<double[]> c =
						modelCache.computeIfAbsent(value.key, k -> new ArrayList<>());
					c.add(value.data);
				}
				if (value.isEob) {
					isConverge &= value.isConverge;
					cnt += 1;
				}
				if (cnt == parallelism) {
					collectModel(out, toCentroids(modelCache), isConverge, value.iterCount);
					cnt = 0;
					modelCache = new HashMap<>();
					isConverge = true;
				}
			}

			private Map<String, double[]> toCentroids(
				Map<String, List<double[]>> newCentroidsCache) {
				Map<String, double[]> newCentroids = new HashMap<>();
				for (Map.Entry<String, List<double[]>> e : newCentroidsCache.entrySet()) {
					ArrayRealVector sum = null;
					for (double[] point : e.getValue()) {
						sum = sum == null ? new ArrayRealVector(point) :
							sum.add(new ArrayRealVector(point));
					}

					newCentroids.put(e.getKey(), sum.mapDivide(e.getValue().size()).toArray());
				}

				return newCentroids;
			}

			private void collectModel(
				Collector<Data> out,
				Map<String, double[]> newCentroids,
				boolean isConverge,
				int iterCnt) {
				System.err.println("----------------------------------");
				System.err.println("reduce collect model iter:" + iterCnt);
				for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
					Data data = new Data();
					data.key = e.getKey();
					data.data = e.getValue();
					//					out.collect(data);
					HeadTailQueue.broadcastOffer(parallelism, data);
					System.err.println("m:" + data.key + ", " + Arrays.toString(data.data));
				}
				System.err.println("----------------------------------");

				Data eob = new Data();
				eob.isEob = true;
				eob.isData = false;
				eob.isConverge = isConverge;
				eob.iterCount = iterCnt + 1;
				//				out.collect(eob);
				HeadTailQueue.broadcastOffer(parallelism, eob);
			}
		});

		sEnv.execute();
	}

	@org.junit.Test
	public void test() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Data> model = sEnv.addSource(new SourceFunction<Data>() {
			@Override
			public void run(SourceContext<Data> ctx) throws Exception {
				while (true) {
					Thread.sleep(60000);
					Data c00 = new Data();
					c00.data = new double[]{0, 0};
					c00.key = "00";
					ctx.collect(c00);
					Data c01 = new Data();
					c01.data = new double[]{0, 1};
					c01.key = "01";
					ctx.collect(c01);
					Data c10 = new Data();
					c10.data = new double[]{1, 0};
					c10.key = "10";
					ctx.collect(c10);
					Data c11 = new Data();
					c11.data = new double[]{1, 1};
					c11.key = "11";
					ctx.collect(c11);
					Data eob = new Data();
					eob.isEob = true;
					eob.isData = false;
					ctx.collect(eob);
				}
			}

			@Override
			public void cancel() {

			}
		});
		DataStream<Data> data = sEnv.addSource(new SourceFunction<Data>() {
			String[] keys = new String[]{"c00", "c01", "c10", "c11"};
			double[][] cs =
				new double[][]{new double[]{0, 0}, new double[]{0, 1}, new double[]{1, 0},
					new double[]{1, 1}};

			@Override
			public void run(SourceContext<Data> ctx) throws Exception {
				while (true) {
					Thread.sleep(10);
					Data d = new Data();
					int idx = (int) (Math.random() * 4);
					d.data = cs[idx];
					d.key = keys[idx];
					d.data = new double[]{d.data[0] + Math.random() * 0.6 - 0.3,
						d.data[1] + Math.random() * 0.6 - 0.3};
					ctx.collect(d);
				}
			}

			@Override
			public void cancel() {

			}
		});

		SplitStream<Data> ss = model.broadcast().flatMap(new RichFlatMapFunction<Data, Data>() {
			List<Data> queue;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				queue = new ArrayList<>();
			}

			@Override
			public void flatMap(Data value, Collector<Data> out) throws Exception {
				if (value.isData) {
					queue.add(value);
				}
				if (value.isEob) {
					while (true) {
						boolean isConverge = value.isConverge;
						for (Data d : queue) {
							d.isOutput = isConverge;
							out.collect(d);
						}
						queue.clear();
						if (!isConverge) {
							out.collect(value);
						} else {
							break;
						}
						while (true) {
							Data d = HeadTailQueue.queue.take();
							if (d.isData) {
								queue.add(d);
							}
							if (d.isEob) {
								value = d;
								break;
							}
						}
					}
				}
			}
		}).split(
			(OutputSelector<Data>) (value) -> Collections
				.singletonList(value.isOutput ? "output" : "iterate"));

		ss.select("output").writeUsingOutputFormat(new PrintingOutputFormat<>());

		DataStream<Data> iter = ss.select("iterate");
		DataStream<Data> delta =
			iter.connect(data).flatMap(new CoFlatMapFunction<Data, Data, Data>() {
				int iterCnt = 0;
				List<Data> model = new LinkedList<>();
				boolean modelReady = false;

				int leastDataCount = 100;
				List<Data> data = new LinkedList<>();
				boolean dataReady = false;

				@Override
				public void flatMap1(Data value, Collector<Data> out) throws Exception {
					if (!value.isEob) {
						model.add(value);
					} else {
						iterCnt = value.iterCount;
						modelReady = true;
					}
					if (dataReady && modelReady) {
						compute(out, model, data, iterCnt);
						modelReady = false;
						dataReady = false;
						iterCnt = 0;
					}
				}

				@Override
				public void flatMap2(Data value, Collector<Data> out) throws Exception {
					data.add(value);
					dataReady = data.size() >= leastDataCount;
					if (dataReady && modelReady) {
						compute(out, model, data, iterCnt);
						modelReady = false;
						dataReady = false;
					}
				}

				private void compute(
					Collector<Data> out,
					List<Data> model,
					List<Data> data,
					int iterCnt) {
					Map<String, double[]> centroids = buildModel(model);
					Map<String, List<double[]>> newCentroidsCache = new HashMap<>();
					for (Data d : data) {
						String nearest = nearest(d, centroids);
						updateCache(nearest, newCentroidsCache, d.data);
					}
					Map<String, double[]> newCentroids = toCentroids(newCentroidsCache);
					boolean isConverge = iterCnt > 3 && isConverge(centroids, newCentroids);
					collectModel(out, newCentroids, isConverge, iterCnt);

					model.clear();
					data.clear();
					//					dataReady = data.size() >= leastDataCount;
				}

				private void collectModel(
					Collector<Data> out,
					Map<String, double[]> newCentroids,
					boolean isConverge,
					int iterCnt) {
					System.err.println("----------------------------------");
					System.err.println("collect model iter:" + iterCnt);
					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
						Data data = new Data();
						data.key = e.getKey();
						data.data = e.getValue();
						//						out.collect(data);
						HeadTailQueue.queue.offer(data);
						System.err.println("m:" + data.key + ", " + Arrays.toString(data.data));
					}
					System.err.println("----------------------------------");

					Data eob = new Data();
					eob.isEob = true;
					eob.isData = false;
					eob.isConverge = isConverge;
					eob.iterCount = iterCnt + 1;
					//					out.collect(eob);
					HeadTailQueue.queue.offer(eob);
				}

				private boolean isConverge(
					Map<String, double[]> centroids,
					Map<String, double[]> newCentroids) {
					for (Map.Entry<String, double[]> e : newCentroids.entrySet()) {
						double[] newC = e.getValue();
						double[] c = centroids.get(e.getKey());
						double dist = new ArrayRealVector(newC).getDistance(new ArrayRealVector(c));
						System.out.print(dist + ",");
						if (dist > 0.1) {
							System.out.println();
							return false;
						}
					}
					System.out.println();
					return true;
				}

				private Map<String, double[]> toCentroids(
					Map<String, List<double[]>> newCentroidsCache) {
					Map<String, double[]> newCentroids = new HashMap<>();
					for (Map.Entry<String, List<double[]>> e : newCentroidsCache.entrySet()) {
						ArrayRealVector sum = null;
						for (double[] point : e.getValue()) {
							sum = sum == null ? new ArrayRealVector(point) :
								sum.add(new ArrayRealVector(point));
						}

						newCentroids.put(e.getKey(), sum.mapDivide(e.getValue().size()).toArray());
					}

					return newCentroids;
				}

				private void updateCache(
					String nearest,
					Map<String, List<double[]>> newCentroidsCache, double[] data) {
					newCentroidsCache.computeIfAbsent(nearest, k -> new ArrayList<>()).add(data);

				}

				private String nearest(Data d, Map<String, double[]> centroids) {
					double[] point = d.data;
					String nearest = null;
					double distance = Double.MAX_VALUE;
					for (Map.Entry<String, double[]> e : centroids.entrySet()) {
						double dist =
							new ArrayRealVector(e.getValue())
								.getDistance(new ArrayRealVector(point));
						if (dist < distance) {
							distance = dist;
							nearest = e.getKey();
						}
					}
					return nearest;
				}

				private Map<String, double[]> buildModel(List<Data> model) {
					Map<String, double[]> map = new HashMap<>();
					for (Data d : model) {
						map.put(d.key, d.data);
					}
					return map;
				}
			});

		sEnv.execute();
	}

	/**
	 *
	 */
	public static class Data {
		public boolean isEob = false;
		public boolean isValue = true; //or delta
		public boolean isOutput = false;
		public boolean isConverge = false;
		public boolean isData = true;
		public int iterCount = 0;
		public String key;
		public double[] data;
	}

	/**
	 *
	 */
	public static class HeadTailQueue {
		public static LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<>();
		public static Map<Integer, LinkedBlockingQueue<Data>> broadcastQueue =
			new HashMap<>();
		private static final Object lock = new Object();

		public static LinkedBlockingQueue<Data> getBroadcastQueue(int key) {
			synchronized (lock) {
				return broadcastQueue.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
			}
		}

		public static void broadcastOffer(int parallelism, Data data) {
			synchronized (lock) {
				for (int i = 0; i < parallelism; i++) {
					broadcastQueue.computeIfAbsent(i, k -> new LinkedBlockingQueue<>()).offer(data);
				}
			}
		}
	}
}
