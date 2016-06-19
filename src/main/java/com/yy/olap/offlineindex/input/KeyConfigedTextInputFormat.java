package com.yy.olap.offlineindex.input;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author colin.ke keqinwu@163.com
 */
public class KeyConfigedTextInputFormat extends FileInputFormat<Text, Text> {
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		String delimiter = context.getConfiguration().get(
				"textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		return new KeyConfigedLineRecordReader(recordDelimiterBytes);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		final CompressionCodec codec =
				new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}
}
