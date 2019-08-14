package org.apache.flink.ds.iter.algo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public interface IterativeAlgorithm<Data, Model, Update, Loss, LossAcc extends Serializable, Result> extends
	Serializable {

	LossAcc createLossAcc();

	boolean isConverge(LossAcc acc, Loss loss);

	Result infer(Data data, Map<String, Model> model);

	Tuple2<Iterable<Update>, Loss> train(Data data, Map<String, Model> model);

	Model merge(Model model, Update update);

	TypeInformation<Model> modelType();

	KeySelector<Model, String> modelKeySelector();

	TypeInformation<Update> updateType();

	KeySelector<Update, String> updateKeySelector();

	TypeInformation<Loss> lossType();

	TypeInformation<Data> dataType();

	KeySelector<Data, String[]> dataKeySelector();

	TypeInformation<Result> resultType();
}
