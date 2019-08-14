package org.apache.flink.ds.iter.iterate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ds.iter.struct.UnifiedModelData;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @param <M>
 * @param <U>
 */
public class FeedbackHeadFlatMap<M, U>
	extends RichFlatMapFunction<UnifiedModelData<M, U>, UnifiedModelData<M, U>> {
	public static Map<Integer, LinkedBlockingQueue> queueMap = new HashMap<>();
	public static ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 100, Long.MAX_VALUE,
		TimeUnit.MINUTES, new LinkedBlockingQueue<>());

	public Boolean running = false;
	public CollectThread thread;
	public final Boolean lock = new Boolean(true);

	public int workerId = -1;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		workerId = getRuntimeContext().getIndexOfThisSubtask();
	}

	public static LinkedBlockingQueue getWorkerQueue(int workerId) {
		return queueMap.computeIfAbsent(workerId, k -> new LinkedBlockingQueue());
	}

	@Override
	public void flatMap(UnifiedModelData<M, U> value,
		Collector<UnifiedModelData<M, U>> out) throws Exception {
		if (!running) {
			synchronized (lock) {
				if (!running) {
					thread = new CollectThread(out);
					executor.execute(thread);
					running = true;
				}
			}
		}
		out.collect(value);
	}

	/**
	 *
	 */
	//TODO not good
	public class CollectThread extends Thread {
		Collector<UnifiedModelData<M, U>> out;

		public CollectThread(Collector<UnifiedModelData<M, U>> out) {
			this.out = out;
		}

		@Override
		public void run() {
			while (true) {
				try {
					out.collect((UnifiedModelData<M, U>) getWorkerQueue(workerId).take());
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
