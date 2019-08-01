package org.apache.flink.ds.iter;

import java.io.Serializable;

/**
 * @param <M>
 * @param <F>
 */
public interface PsMerger<M, F> extends Serializable {
	M merge(M model, F feedback);
}
