package com.yy.olap.offlineindex.output;

import com.yy.olap.offlineindex.utils.Utils;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author colin.ke keqinwu@163.com
 */
public class HdfsSyncingLocalFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

	private HdfsSyncingLocalFileOutputCommitter committer;

	@Override
	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
		if (committer == null) {
			committer = new HdfsSyncingLocalFileOutputCommitter(Utils.getTmpDirPath(), getOutputPath(context), context);
		}

		return committer;
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new RecordWriter<K, V>() {
			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			}

			@Override
			public void write(K key, V val) throws IOException, InterruptedException {
			}
		};
	}
}
