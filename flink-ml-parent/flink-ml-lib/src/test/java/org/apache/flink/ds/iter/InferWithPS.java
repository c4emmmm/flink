package org.apache.flink.ds.iter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ds.iter.struct.ModelWrapper;
import org.apache.flink.ds.iter.struct.UnifiedModelData;
import org.apache.flink.ds.iter.struct.UpdateWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import java.util.Map;

/**
 *
 */
public class InferWithPS {

	public static <Data, Model, Update, Result> DataStream<Result> infer(
		DataStream<Model> initialModel,
		DataStream<Update> update,
		DataStream<Data> data,
		StreamTransformer<Tuple2<Data, Map<String, Model>>, Result> infer,
		StateParamService<Data, Model, Update> ps) {

		DataStream<UnifiedModelData<Model, Update>> unifiedModelInput =
			unifyInitialModelAndModelUpdate(initialModel, update, ps.modelType, ps.updateType);

		Tuple2<DataStream<Tuple2<Data, Map<String, Model>>>,
			SingleOutputStreamOperator<?>> joinResult = ps.updateOrJoin(unifiedModelInput, data);
		DataStream<Tuple2<Data, Map<String, Model>>> fullData = joinResult.f0;

		return infer.transform(fullData);
	}

	private static <Model, Update> DataStream<UnifiedModelData<Model, Update>> unifyInitialModelAndModelUpdate(
		@Nullable DataStream<Model> initialModel,
		DataStream<Update> update,
		TypeInformation<Model> modelType,
		TypeInformation<Update> updateType) {
		TypeInformation<UnifiedModelData<Model, Update>> unifiedModelType =
			UnifiedModelData.returnType(modelType, updateType);

		DataStream<UnifiedModelData<Model, Update>> unifiedInput =
			update.map(new UpdateWrapper<Model, Update>()).returns(unifiedModelType);
		if (initialModel != null) {
			DataStream<UnifiedModelData<Model, Update>> wrappedModel =
				initialModel.map(new ModelWrapper<Model, Update>()).returns(unifiedModelType);
			unifiedInput = unifiedInput.union(wrappedModel);
		}
		return unifiedInput;
	}
}
