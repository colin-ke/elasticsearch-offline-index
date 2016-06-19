package com.yy.olap.offlineindex.input;

import com.yy.olap.offlineindex.exceptions.ConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * @author colin.ke keqinwu@163.com
 */
public class KeyConfigedLineRecordReader extends RecordReader<Text, Text> {
	private static final Log LOG = LogFactory.getLog(KeyConfigedLineRecordReader.class);
	public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";

	private long start;
	private long pos;
	private long end;
	private SplitLineReader in;
	private FSDataInputStream fileIn;
	private Seekable filePosition;
	private int maxLineLength;
	private Text key;
	private Text value;
	private boolean isCompressedInput;
	private Decompressor decompressor;
	private byte[] recordDelimiterBytes;

	public KeyConfigedLineRecordReader() {
	}

	public KeyConfigedLineRecordReader(byte[] recordDelimiter) {
		this.recordDelimiterBytes = recordDelimiter;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		key = new Text(resolveKey(file, job));

		// open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(file);

		CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
		if (null != codec) {
			isCompressedInput = true;
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec)
						.createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn;
			} else {
				in = new SplitLineReader(codec.createInputStream(fileIn, decompressor), job, this.recordDelimiterBytes);
				filePosition = fileIn;
			}
		} else {
			fileIn.seek(start);
			in = new SplitLineReader(fileIn, job, this.recordDelimiterBytes);
			filePosition = fileIn;
		}
		// If this is not the first split, we always throw away first record
		// because we always (except the last split) read one extra line in
		// next() method.
		if (start != 0) {
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
		this.pos = start;
	}

	private String resolveKey(Path file, Configuration config) {
		String path = file.toUri().getPath();
		String parentPath = file.getParent().toUri().getPath();
		for (Map.Entry<String, String> entry : config) {
			System.out.println(entry.getKey() + "->" + entry.getValue());
		}
		LOG.info("for input path - " + path);
		String type = config.get(path);
		if(null == type)
			type = config.get(parentPath);
		LOG.info("type is " + type);
		if(null == type)
			throw new ConfigException("the type for input:" + path + " is not set!");

		return type;

	}


	private int maxBytesToConsume(long pos) {
		return isCompressedInput ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	public boolean nextKeyValue() throws IOException {
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		// We always read one extra line, which lies outside the upper
		// split limit i.e. (end - 1)
		while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
			newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public Text getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
	}
}
