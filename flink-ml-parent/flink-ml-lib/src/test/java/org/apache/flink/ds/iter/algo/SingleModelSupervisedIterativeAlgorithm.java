package org.apache.flink.ds.iter.algo;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * @param <Feature>
 * @param <Label>
 * @param <Model>
 * @param <Update>
 * @param <Loss>
 * @param <LossAcc>
 */
public interface SingleModelSupervisedIterativeAlgorithm
	<Feature, Label, Model, Update, Loss, LossAcc extends Serializable> extends
	SupervisedIterativeAlgorithm<Feature, Label, Model, Update, Loss, LossAcc> {
	String SINGLETON_MODEL = "SINGLETON_MODEL";

	Label predict(Feature feature, Model model);

	Tuple2<Update, Loss> train(Feature feature, Label label, Model model);

	@Override
	default Label predict(Feature feature, Map<String, Model> model) {
		Model m = model.get(SINGLETON_MODEL);
		return m == null ? null : predict(feature, m);
	}

	@Override
	default Tuple2<Iterable<Update>, Loss> train(Feature feature, Label label,
		Map<String, Model> model) {
		Model m = model.get(SINGLETON_MODEL);
		Tuple2<Update, Loss> res = m == null ? null : train(feature, label, m);
		return res == null ? null : new Tuple2<>(Collections.singleton(res.f0), res.f1);
	}

	@Override
	default KeySelector<Feature, String[]> featureKeySelector() {
		return (KeySelector<Feature, String[]>) value -> new String[]{SINGLETON_MODEL};
	}

	@Override
	default KeySelector<Model, String> modelKeySelector() {
		return (KeySelector<Model, String>) value -> SINGLETON_MODEL;
	}

	@Override
	default KeySelector<Update, String> updateKeySelector() {
		return (KeySelector<Update, String>) value -> SINGLETON_MODEL;
	}
}
