package org.apache.flink.ds.iter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ds.iter.iterate.FeedbackHeadFlatMap;
import org.apache.flink.ds.iter.iterate.FeedbackTailFlatMap;
import org.apache.flink.ds.iter.keyed.BroadcastConvergeSignal;
import org.apache.flink.ds.iter.struct.ConvergeSignal;
import org.apache.flink.ds.iter.struct.ConvergeSignalWrapper;
import org.apache.flink.ds.iter.struct.ModelWrapper;
import org.apache.flink.ds.iter.struct.UnifiedModelData;
import org.apache.flink.ds.iter.struct.UpdateWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 *
 */
public class IterativeTrainWithPS {

	public static <Data, Model, Update, Loss> DataStream<Model> iterativeTrain(
		DataStream<Model> initialModel,
		DataStream<Data> data,
		TwoOutputStreamTransformer<Tuple2<Data, Map<String, Model>>, Update, Loss> train,
		StreamTransformer<Loss, ConvergeSignal> judgeConverge,
		StateParamService<Data, Model, Update> ps) {

		TypeInformation<UnifiedModelData<Model, Update>> unifiedModelType =
			UnifiedModelData.returnType(ps.modelType, ps.updateType);

		//wrap model
		DataStream<UnifiedModelData<Model, Update>> wrapModel =
			initialModel.map(new ModelWrapper<Model, Update>()).returns(unifiedModelType);

		//iterate start, output is unified model data including model, update and converge signal
		DataStream<UnifiedModelData<Model, Update>> unifiedModelInput =
			wrapModel.flatMap(new FeedbackHeadFlatMap<>());

		//join data with model or update model
		Tuple2<DataStream<Tuple2<Data, Map<String, Model>>>,
			SingleOutputStreamOperator<?>> joinResult = ps.updateOrJoin(unifiedModelInput, data);
		DataStream<Tuple2<Data, Map<String, Model>>> fullData = joinResult.f0;
		SingleOutputStreamOperator<?> psOperator = joinResult.f1;

		//train and get the model update, update should be broadcast
		Tuple2<DataStream<Update>, DataStream<Loss>> trainResult = train.transform(fullData);
		DataStream<Update> update = trainResult.f0;
		DataStream<Loss> loss = trainResult.f1;

		//wrap update
		DataStream<UnifiedModelData<Model, Update>> wrapUpdate =
			update.map(new UpdateWrapper<Model, Update>()).returns(unifiedModelType);

		//check if converge and wrap converge signal
		DataStream<UnifiedModelData<Model, Update>> wrapConvergeSignal =
			judgeConverge.transform(loss)
				.flatMap(new BroadcastConvergeSignal(ps.psParallelism))
				.map(new ConvergeSignalWrapper<Model, Update>())
				.returns(unifiedModelType);

		//end iteration and feedback
		wrapUpdate.union(wrapConvergeSignal).flatMap(new FeedbackTailFlatMap<>());

		//get and output model
		return psOperator.getSideOutput(new OutputTag<>("model", ps.modelType));
	}
}
