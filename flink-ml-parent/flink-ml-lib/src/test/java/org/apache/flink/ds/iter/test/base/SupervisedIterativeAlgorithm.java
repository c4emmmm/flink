package org.apache.flink.ds.iter.test.base;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 *
 */
public interface SupervisedIterativeAlgorithm<Model, Update, Feature, Label> extends
	IterativeAlgorithm<Model, Update, Tuple2<Feature, Label>, Label> {

	Label infer(Feature feature, Label label, Model model);

	Update train(Feature feature, Label label, Model model);

	TypeInformation<Feature> featureType();

	TypeInformation<Label> labelType();

	@Override
	default Label infer(Tuple2<Feature, Label> fullData, Model model) {
		return infer(fullData.f0, fullData.f1, model);
	}

	@Override
	default Update train(Tuple2<Feature, Label> fullData, Model model) {
		return train(fullData.f0, fullData.f1, model);
	}

	@Override
	default TypeInformation<Tuple2<Feature, Label>> dataType() {
		return new TupleTypeInfo<>(featureType(), labelType());
	}

	@Override
	default TypeInformation<Label> resultType() {
		return labelType();
	}

}
