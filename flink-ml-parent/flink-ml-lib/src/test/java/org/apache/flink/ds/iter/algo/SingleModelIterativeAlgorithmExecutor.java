package org.apache.flink.ds.iter.algo;

import java.io.Serializable;

/**
 *
 */
public class SingleModelIterativeAlgorithmExecutor<Data, Model, Update, Loss, LossAcc extends Serializable,
	Result> extends IterativeAlgorithmExecutor<Data, Model, Update, Loss, LossAcc, Result> {

	public SingleModelIterativeAlgorithmExecutor(IterativeAlgorithm<Data, Model, Update, Loss, LossAcc, Result> algo,
		int psParallelism) {
		super(algo, "broadcast", psParallelism);
	}
}
