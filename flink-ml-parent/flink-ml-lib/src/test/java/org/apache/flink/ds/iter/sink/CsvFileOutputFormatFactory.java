package org.apache.flink.ds.iter.sink;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

/**
 * @param <T>
 */
public abstract class CsvFileOutputFormatFactory<T extends Tuple> implements
	FileOutputFormatFactory<T> {

	private String rootPath;

	public CsvFileOutputFormatFactory(String rootPath) {
		this.rootPath = rootPath;
	}

	@Override
	public FileOutputFormat<T> createFormat(String version) {
		String path = getPath(version);
		CsvOutputFormat<T> format = new CsvOutputFormat<>(new Path(path));
		configure(format);
		return format;
	}

	private String getPath(String version) {
		return rootPath + "/" + version;
	}

	public abstract void configure(CsvOutputFormat<T> format);
}
