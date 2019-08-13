package org.apache.flink.ds.iter.struct;

import java.io.Serializable;

/**
 *
 */
public class ConvergeSignal implements Serializable {
	public int targetWorker;
	public boolean isConverge;
	public String versionId;

	private ConvergeSignal(int targetWorker, boolean isConverge, String versionId) {
		this.targetWorker = targetWorker;
		this.isConverge = isConverge;
		this.versionId = versionId;
	}

	public static ConvergeSignal create() {
		return create(String.valueOf(System.currentTimeMillis()));
	}

	public static ConvergeSignal create(String versionId) {
		return create(0, versionId);
	}

	public static ConvergeSignal create(int targetWorker, String versionId) {
		return new ConvergeSignal(targetWorker, true, versionId);
	}
}
