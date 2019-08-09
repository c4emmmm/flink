package org.apache.flink.ds.iter.sink;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MultiVersionFileSink<IT, DT> extends RichSinkFunction<IT> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(MultiVersionFileSink.class);

	private VersionRecordProcessor<IT, DT> recordProcessor;
	private FileOutputFormatFactory<DT> formatFactory;
	private Map<String, FileOutputFormat<DT>> formats = new HashMap<>();

	private Configuration parameters;
	private int indexOfThisSubtask = -1;
	private int numberOfParallelSubtasks = -1;

	private String recentVersion;

	public MultiVersionFileSink(FileOutputFormatFactory<DT> formatFactory,
		VersionRecordProcessor<IT, DT> recordProcessor) {
		this.formatFactory = formatFactory;
		this.recordProcessor = recordProcessor;
	}

	@Override
	public void open(Configuration parameters) {
		this.parameters = parameters;
		this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
		this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
	}

	@Override
	public void invoke(IT record, Context context) throws Exception {
		String version = recordProcessor.getVersion(record);
		FileOutputFormat<DT> format = formats.get(version);
		if (format == null) {
			format = initNewFormat(version);
			formats.put(version, format);
		}
		if (recentVersion == null) {
			recentVersion = version;
		}
		if (!recentVersion.equals(version)) {
			FileOutputFormat<DT> recentFormat = formats.get(recentVersion);
			closeFormat(recentFormat);
			formats.remove(recentVersion);
			recentVersion = version;
		}
		format.writeRecord(recordProcessor.getData(record));
	}

	private FileOutputFormat<DT> initNewFormat(String version) throws IOException {
		FileOutputFormat<DT> f = formatFactory.createFormat(version);
		f.setRuntimeContext(getRuntimeContext());
		f.configure(parameters);
		f.open(indexOfThisSubtask, numberOfParallelSubtasks);
		return f;
	}

	@Override
	public void close() throws IOException {
		for (FileOutputFormat<DT> format : formats.values()) {
			closeFormat(format);
		}
	}

	private void closeFormat(FileOutputFormat<DT> format) throws IOException {
		try {
			format.close();
		} catch (Exception ex) {
			try {
				format.tryCleanupOnError();
			} catch (Throwable t) {
				LOG.error("Cleanup on error failed.", t);
			}
			throw ex;
		}
	}
}
