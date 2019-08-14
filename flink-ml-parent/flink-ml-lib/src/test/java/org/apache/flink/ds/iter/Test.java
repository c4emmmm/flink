package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ds.iter.algo.IterativeAlgorithm;
import org.apache.flink.ds.iter.algo.IterativeAlgorithmExecutor;
import org.apache.flink.ds.iter.algo.SingleModelIterativeAlgorithmExecutor;
import org.apache.flink.ds.iter.algo.lr.ComplexLR;
import org.apache.flink.ds.iter.algo.lr.DoubleAcc;
import org.apache.flink.ds.iter.algo.lr.SimpleLR;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.google.gson.Gson;

/**
 *
 */
public class Test {

	@org.junit.Test
	public void testSimpleLR() throws Exception {
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
		IterativeAlgorithm<Tuple2<double[], Double>, double[], double[], Double, DoubleAcc, Tuple2<double[], Double>>
			algo = new SimpleLR();
		IterativeAlgorithmExecutor<Tuple2<double[], Double>, double[], double[], Double, DoubleAcc, Tuple2<double[], Double>>
			lrExecutor = new SingleModelIterativeAlgorithmExecutor<>(algo, 3);

		DataStream<Tuple2<double[], Double>> data = getDataSource(sEnv, 1);
		DataStream<double[]> model = lrExecutor.fit(initialModel, data);

		model.flatMap((FlatMapFunction<double[], Boolean>) (value, out) ->
			System.err.println("model=" + new Gson().toJson(value)))
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		DataStream<Tuple2<double[], Double>> inferData = getDataSource(sEnv, 1000);
		DataStream<Tuple2<double[], Double>> result =
			lrExecutor.transform(initialModel, model, inferData);

		result.flatMap((FlatMapFunction<Tuple2<double[], Double>, Boolean>) (value, out) -> {
			double[] d = value.f0;
			System.err.println("result=" + new Gson().toJson(value) + ", label=" +
				(d[0] * 11.1 + d[1] * 17.3 + d[2] * 7.7 + 23));
		}).returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		sEnv.execute();
	}

	@org.junit.Test
	public void testComplexLR() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(3);

		IterativeAlgorithm<Tuple2<double[], Double>, Tuple2<Integer, Double>, Tuple2<Integer,
			Double>, Double, DoubleAcc, Tuple2<double[], Double>> algo = new ComplexLR();
		IterativeAlgorithmExecutor<Tuple2<double[], Double>, Tuple2<Integer, Double>, Tuple2<Integer,
			Double>, Double, DoubleAcc, Tuple2<double[], Double>> lrExecutor =
			new IterativeAlgorithmExecutor<>(algo, "keyed", 3);
		DataStream<Tuple2<Integer, Double>> initialModel = getModelSource(sEnv);

		DataStream<Tuple2<double[], Double>> data = getDataSource(sEnv, 1);
		DataStream<Tuple2<Integer, Double>> model = lrExecutor.fit(initialModel, data);

		model.flatMap((FlatMapFunction<Tuple2<Integer, Double>, Boolean>) (value, out) ->
			System.err.println("model=" + new Gson().toJson(value)))
			.returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		DataStream<Tuple2<double[], Double>> inferData = getDataSource(sEnv, 1000);
		DataStream<Tuple2<double[], Double>> result =
			lrExecutor.transform(initialModel, model, inferData);

		result.flatMap((FlatMapFunction<Tuple2<double[], Double>, Boolean>) (value, out) -> {
			double[] d = value.f0;
			System.err.println("result=" + new Gson().toJson(value) + ", label=" +
				(d[0] * 11.1 + d[1] * 17.3 + d[2] * 7.7 + 23));
		}).returns(BasicTypeInfo.BOOLEAN_TYPE_INFO);

		sEnv.execute();
	}

	//for test
	public DataStream<Tuple2<double[], Double>> getDataSource(StreamExecutionEnvironment sEnv,
		int interval) {
		return sEnv.addSource(new SourceFunction<Tuple2<double[], Double>>() {
			@Override
			public void run(SourceContext<Tuple2<double[], Double>> ctx) throws Exception {
				while (true) {
					if (interval > 0) {
						Thread.sleep(interval);
					}
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
