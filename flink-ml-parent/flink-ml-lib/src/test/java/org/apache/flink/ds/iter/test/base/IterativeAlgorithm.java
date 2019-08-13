package org.apache.flink.ds.iter.test.base;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;

/**
 *
 */
public interface IterativeAlgorithm<Model, Update, Data, Result> extends Serializable {

	boolean isConverge(Update update);

	Result infer(Data data, Model model);

	Update train(Data data, Model model);

	Model merge(Model model, Update update);

	TypeInformation<Model> modelType();

	TypeInformation<Update> updateType();

	TypeInformation<Data> dataType();

	TypeInformation<Result> resultType();
}
