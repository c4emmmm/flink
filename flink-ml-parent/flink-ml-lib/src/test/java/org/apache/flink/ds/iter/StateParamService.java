package org.apache.flink.ds.iter;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.ds.iter.broadcast.BroadcastPsCoProcessor;
import org.apache.flink.ds.iter.keyed.AssignDataUUIDAndExtractKeys;
import org.apache.flink.ds.iter.keyed.FlattenDataKey;
import org.apache.flink.ds.iter.keyed.KeyedPsCoProcessor;
import org.apache.flink.ds.iter.keyed.MergeDataFlatMap;
import org.apache.flink.ds.iter.keyed.PartitionKeySelector;
import org.apache.flink.ds.iter.keyed.UnifiedModelInputPartitionKeySelector;
import org.apache.flink.ds.iter.struct.UnifiedModelData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

/**
 *
 */
public abstract class StateParamService<Data, Model, Update> {
	protected TypeInformation<Model> modelType;
	protected KeySelector<Model, String> modelKeySelector;
	protected TypeInformation<Update> updateType;
	protected KeySelector<Update, String> updateKeySelector;
	protected PsMerger<Model, Update> merger;
	protected TypeInformation<Data> dataType;
	protected KeySelector<Data, String[]> dataKeySelector;
	protected int psParallelism;

	public StateParamService(TypeInformation<Model> modelType,
		KeySelector<Model, String> modelKeySelector,
		TypeInformation<Update> updateType,
		KeySelector<Update, String> updateKeySelector,
		PsMerger<Model, Update> merger,
		TypeInformation<Data> dataType,
		KeySelector<Data, String[]> dataKeySelector,
		int psParallelism) {
		this.modelType = modelType;
		this.modelKeySelector = modelKeySelector;
		this.updateType = updateType;
		this.updateKeySelector = updateKeySelector;
		this.merger = merger;
		this.dataType = dataType;
		this.dataKeySelector = dataKeySelector;
		this.psParallelism = psParallelism;
	}

	public abstract Tuple2<DataStream<Tuple2<Data, Map<String, Model>>>,
		SingleOutputStreamOperator<?>> updateOrJoin(
		DataStream<UnifiedModelData<Model, Update>> unifiedModelStream,
		DataStream<Data> data);

	public static <Data, Model, Update> StateParamService<Data, Model, Update> getPS(
		String type,
		TypeInformation<Model> modelType,
		KeySelector<Model, String> modelKeySelector,
		TypeInformation<Update> updateType,
		KeySelector<Update, String> updateKeySelector,
		PsMerger<Model, Update> merger,
		TypeInformation<Data> dataType,
		KeySelector<Data, String[]> dataKeySelector,
		int psParallelism) {
		if ("broadcast".equals(type)) {
			return new BroadcastPS<>(modelType, modelKeySelector, updateType, updateKeySelector,
				merger, dataType, dataKeySelector,
				psParallelism);
		} else if ("keyed".equals(type)) {
			return new KeyedPS<>(modelType, modelKeySelector, updateType, updateKeySelector,
				merger, dataType, dataKeySelector,
				psParallelism);
		} else {
			throw new RuntimeException();
		}
	}

	/**
	 *
	 */
	public static class BroadcastPS<Data, Model, Update> extends
		StateParamService<Data, Model, Update> {
		public BroadcastPS(TypeInformation<Model> modelType,
			KeySelector<Model, String> modelKeySelector,
			TypeInformation<Update> updateType,
			KeySelector<Update, String> updateKeySelector,
			PsMerger<Model, Update> merger,
			TypeInformation<Data> dataType,
			KeySelector<Data, String[]> dataKeySelector, int psParallelism) {
			super(modelType, modelKeySelector, updateType, updateKeySelector, merger, dataType,
				dataKeySelector, psParallelism);
		}

		@Override
		public Tuple2<DataStream<Tuple2<Data, Map<String, Model>>>, SingleOutputStreamOperator<?>> updateOrJoin(
			DataStream<UnifiedModelData<Model, Update>> unifiedModelStream,
			DataStream<Data> data) {
			SingleOutputStreamOperator<Tuple2<Data, Map<String, Model>>> psOperator =
				unifiedModelStream.broadcast().connect(data)
					.process(new BroadcastPsCoProcessor<>(merger,
						modelKeySelector, updateKeySelector, dataKeySelector, modelType))
					.setParallelism(psParallelism);
			return new Tuple2<>(psOperator, psOperator);
		}
	}

	/**
	 *
	 */
	public static class KeyedPS<Data, Model, Update> extends
		StateParamService<Data, Model, Update> {
		public KeyedPS(TypeInformation<Model> modelType,
			KeySelector<Model, String> modelKeySelector,
			TypeInformation<Update> updateType,
			KeySelector<Update, String> updateKeySelector,
			PsMerger<Model, Update> merger,
			TypeInformation<Data> dataType,
			KeySelector<Data, String[]> dataKeySelector, int psParallelism) {
			super(modelType, modelKeySelector, updateType, updateKeySelector, merger, dataType,
				dataKeySelector, psParallelism);
		}

		public Tuple2<DataStream<Tuple2<Data, Map<String, Model>>>,
			SingleOutputStreamOperator<?>> updateOrJoin(
			DataStream<UnifiedModelData<Model, Update>> unifiedModelStream,
			DataStream<Data> data) {
			//flatten data key
			DataStream<Tuple3<Long, String[], Data>> sampleDataWithUUID =
				data.flatMap(new AssignDataUUIDAndExtractKeys<>(dataKeySelector));
			DataStream<Tuple2<Long, String>> sampleDataKey =
				sampleDataWithUUID.flatMap(new FlattenDataKey<>());

			//join data with model, merge update to model on the other side
			SingleOutputStreamOperator<Tuple3<Long, String, Model>> psOperator =
				unifiedModelStream
					.keyBy(new UnifiedModelInputPartitionKeySelector<>(modelKeySelector,
						updateKeySelector, psParallelism))
					.connect(
						sampleDataKey.keyBy(new PartitionKeySelector<>(f -> f.f1, psParallelism)))
					.process(new KeyedPsCoProcessor<>(merger,
						modelKeySelector, updateKeySelector,
						modelType)).returns(
					new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO,
						modelType)).setParallelism(psParallelism);

			DataStream<Tuple3<Long, String, Model>> joinResult = psOperator;

			//merge flatten data with model into original data
			DataStream<Tuple2<Data, Map<String, Model>>> fullData =
				sampleDataWithUUID.keyBy(f -> f.f0).connect(joinResult.keyBy(f -> f.f0))
					.flatMap(new MergeDataFlatMap<>(dataType, modelType));

			//Model can be acquired only in psOperator, so return both fullData and psOperator
			return new Tuple2<>(fullData, psOperator);
		}
	}
}
