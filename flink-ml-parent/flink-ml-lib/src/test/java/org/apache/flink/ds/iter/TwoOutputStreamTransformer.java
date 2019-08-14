package org.apache.flink.ds.iter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * @param <IN>
 * @param <OUT1>
 * @param <OUT2>
 */
public interface TwoOutputStreamTransformer<IN, OUT1, OUT2> extends Serializable {
	Tuple2<DataStream<OUT1>, DataStream<OUT2>> transform(DataStream<IN> in);
}
