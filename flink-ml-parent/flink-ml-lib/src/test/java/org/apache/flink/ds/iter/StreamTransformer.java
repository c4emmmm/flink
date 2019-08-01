package org.apache.flink.ds.iter;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * @param <IN>
 * @param <OUT>
 */
public interface StreamTransformer<IN, OUT> extends Serializable {
	DataStream<OUT> transform(DataStream<IN> in);
}
