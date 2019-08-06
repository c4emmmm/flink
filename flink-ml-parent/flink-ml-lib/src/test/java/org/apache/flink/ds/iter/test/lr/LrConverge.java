package org.apache.flink.ds.iter.test.lr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *
 */
public class LrConverge implements FlatMapFunction<Tuple2<Integer, Double>, Boolean> {

	int inputCount = 0;
	int convergeCount = 0;

	@Override
	public void flatMap(Tuple2<Integer, Double> value, Collector<Boolean> out) throws Exception {
		double loss = value.f1;
		inputCount++;
		if (convergeCount < 1000) {
			if (inputCount % 100 == 0) {
				System.out.println(
					"curIter:" + inputCount + ", cur count=" + convergeCount + ", cur loss=" +
						loss);
			}
			if (Math.abs(loss) < 0.001) {
				convergeCount++;
			} else {
				convergeCount = convergeCount <= 1 ? 0 : convergeCount - 1;
			}
			if (convergeCount == 1000) {
				System.out.println("iter=" + inputCount + ", seems converge");
				out.collect(true);
			}
		}
	}
}
