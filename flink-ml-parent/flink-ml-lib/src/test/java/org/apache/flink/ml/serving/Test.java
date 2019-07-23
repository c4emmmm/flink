package org.apache.flink.ml.serving;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 *
 */
public class Test {
	@org.junit.Test
	public void test() throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);
		CsvTableSource source =
			new CsvTableSource("/Users/hidden/Temp/als/", new String[]{"a", "b", "c"},
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
					BasicTypeInfo.DOUBLE_TYPE_INFO});
		TableSink sink = new PrinterTableSink().configure(new String[]{"a", "b", "c"},
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO});
		tEnv.registerTableSource("source", source);
		tEnv.registerTableSink("sink", sink);

		tEnv.scan("source").insertInto("sink");
		tEnv.execute("");
	}

	public static class PrinterTableSink implements AppendStreamTableSink<Row> {

		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		@Override
		public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			PrinterTableSink configuredSink = new PrinterTableSink();
			configuredSink.fieldNames = fieldNames;
			configuredSink.fieldTypes = fieldTypes;
			return configuredSink;
		}

		@Override
		public String[] getFieldNames() {
			return fieldNames;
		}

		@Override
		public TypeInformation<?>[] getFieldTypes() {
			return fieldTypes;
		}

		@Override
		public TypeInformation<Row> getOutputType() {
			return new RowTypeInfo(getFieldTypes(), getFieldNames());
		}

		@Override
		public void emitDataStream(DataStream<Row> dataStream) {
			consumeDataStream(dataStream);
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
			return dataStream.addSink(new OutputFormatSinkFunction<>(new PrinterOutputFormat()));
		}
	}

	public static class PrinterOutputFormat implements OutputFormat<Row> {

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {

		}

		@Override
		public void writeRecord(Row record) throws IOException {
			System.out.println(record.toString());
		}

		@Override
		public void close() throws IOException {

		}
	}
}
