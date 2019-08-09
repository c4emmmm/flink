package org.apache.flink.ds.iter.sink;

import java.io.Serializable;

/**
 * @param <DT>
 * @param <IT>
 */
public interface VersionRecordProcessor<IT, DT> extends Serializable {

	String getVersion(IT record);

	DT getData(IT record);
}
