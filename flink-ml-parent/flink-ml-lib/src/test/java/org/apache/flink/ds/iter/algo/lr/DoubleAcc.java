package org.apache.flink.ds.iter.algo.lr;

import java.io.Serializable;

/**
 *
 */
public class DoubleAcc implements Serializable {
	double value;

	int sinceLastConverge = Integer.MAX_VALUE;
}
