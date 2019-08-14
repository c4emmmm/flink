package org.apache.flink.ds.iter;

import java.io.Serializable;

/**
 * @param <M>
 * @param <U>
 */
public interface PsMerger<M, U> extends Serializable {
	M merge(M model, U update);
}
