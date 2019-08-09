package org.apache.flink.ds.iter.sink;

import org.apache.flink.api.common.io.FileOutputFormat;

import java.io.Serializable;

/**
 *
 */
public interface FileOutputFormatFactory<IT> extends Serializable {
	FileOutputFormat<IT> createFormat(String version);
}
