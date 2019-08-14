package org.apache.flink.ds.iter.algo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public interface SupervisedIterativeAlgorithm
	<Feature, Label, Model, Update, Loss, LossAcc extends Serializable> extends
	IterativeAlgorithm<Tuple2<Feature, Label>, Model, Update, Loss, LossAcc, Tuple2<Feature, Label>> {

	Label predict(Feature feature, Map<String, Model> model);

	Tuple2<Iterable<Update>, Loss> train(Feature feature, Label label, Map<String, Model> model);

	TypeInformation<Feature> featureType();

	TypeInformation<Label> labelType();

	KeySelector<Feature, String[]> featureKeySelector();

	@Override
	default Tuple2<Feature, Label> infer(Tuple2<Feature, Label> input, Map<String, Model> model) {
		Label label = predict(input.f0, model);
		return label == null ? null : new Tuple2<>(input.f0, predict(input.f0, model));
	}

	@Override
	default Tuple2<Iterable<Update>, Loss> train(Tuple2<Feature, Label> input,
		Map<String, Model> model) {
		return train(input.f0, input.f1, model);
	}

	@Override
	default TypeInformation<Tuple2<Feature, Label>> dataType() {
		return new TupleTypeInfo<>(featureType(), labelType());
	}

	@Override
	default KeySelector<Tuple2<Feature, Label>, String[]> dataKeySelector() {
		return (KeySelector<Tuple2<Feature, Label>, String[]>)
			value -> featureKeySelector().getKey(value.f0);
	}

	@Override
	default TypeInformation<Tuple2<Feature, Label>> resultType() {
		return new TupleTypeInfo<>(featureType(), labelType());
	}
}
