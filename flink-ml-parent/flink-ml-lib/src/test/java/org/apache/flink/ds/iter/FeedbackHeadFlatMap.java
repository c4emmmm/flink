package org.apache.flink.ds.iter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @param <M>
 * @param <F>
 */
public class FeedbackHeadFlatMap<M, F>
	extends RichFlatMapFunction<M, ModelOrFeedback<M, F>> {
	public static LinkedBlockingQueue queue = new LinkedBlockingQueue();
	public Boolean running = false;
	public CollectThread thread;
	public final Boolean lock = new Boolean(true);
	public static ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE,
		TimeUnit.MINUTES,
		new LinkedBlockingQueue<>());

	@Override
	public void flatMap(M value, Collector<ModelOrFeedback<M, F>> out) throws Exception {
		System.out.println("receive init model:" + value);
		if (!running) {
			synchronized (lock) {
				if (!running) {
					thread = new CollectThread(out);
					executor.execute(thread);
					running = true;
				}
			}
		}
		System.out.println("can init model:" + value);
		out.collect(new ModelOrFeedback<>(true, value, null));
	}

	/**
	 *
	 */
	public class CollectThread extends Thread {
		Collector<ModelOrFeedback<M, F>> out;

		public CollectThread(Collector<ModelOrFeedback<M, F>> out) {
			this.out = out;
		}

		@Override
		public void run() {
			while (true) {
				try {
					F feedback = (F) queue.take();
					System.out.println("receive feedback:" + feedback);
					out.collect(new ModelOrFeedback<>(false, null, feedback));
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
