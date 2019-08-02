package org.apache.flink.ds.iter.keyed;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @param <D>
 * @param <M>
 */
public class MergeDataFlatMap<D, M> extends
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
			new Tuple3<>(value.f2, new HashSet<>(Arrays.asList(value.f1)), new HashMap<>());
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
