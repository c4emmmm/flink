package org.apache.flink.ml.serving;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 *
 */
public class ServingOp implements CoFlatMapFunction<Row, Row, Row> {
	private ValueState servingVersion;
	private MapState model;
	private boolean inited = false;

	@Override
	public void flatMap1(Row value, Collector<Row> out) throws Exception {
		if (!inited) {
			init();
		}

	}

	@Override
	public void flatMap2(Row value, Collector<Row> out) throws Exception {
		if (!inited) {
			init();
		}

		RowModel model = new RowModel(value);
		int version = model.getVersion();
	}

	private void init() {
		inited = true;
	}

	/**
	 *
	 */
	private static class RowModel {

		private final Row row;

		public RowModel(Row row) {
			this.row = row;
		}

		public int getVersion() {
			return 0;
		}
	}
}
