package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.exceptions.OfflineIndexException;
import com.yy.olap.offlineindex.input.KeyConfigedTextInputFormat;
import com.yy.olap.offlineindex.output.HdfsSyncingLocalFileOutputFormat;
import com.yy.olap.offlineindex.utils.ConfigUtil;
import com.yy.olap.offlineindex.utils.MultiThreadCopyHelper;
import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author colin.ke keqinwu@163.com
 */
public class JobMain {

	private static Log logger = LogFactory.getLog("JOB_MAIN");
	private static final String TMP_IO_PREFIX = "/tmp/es_offline_index/";
	private static final String OUTPUT_DIR = TMP_IO_PREFIX + "output_" + System.nanoTime();
	private static MultiThreadCopyHelper copyHelper;

	static {
		Utils.registeredURLStreamHandler();
	}

	public static void main(String[] args) throws Exception {
		logArgs(args);
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		JobHelper jobHelper = null;
		int numberOfShards;
		List<Path> inputPaths = new ArrayList<>();
		copyHelper = new MultiThreadCopyHelper(FileSystem.getLocal(config), hdfs, 12, MultiThreadCopyHelper.CopyType.UPLOAD);
		String esIndicesPath;
		String esAddress;

		if (args.length == 1) {
			//使用properties配置，可配置多个type，以及相应的多个input
			IndexContext indexContext = IndexContext.fromProperties(args[0]);
			for (TypeContext type : indexContext.getTypes()) {

				Path hdfsInputPath = ensureOnHdfs(new URL(type.getInputPath()), hdfs);
				inputPaths.add(hdfsInputPath);

				// inputPath -> type
				// so that we can pass the typeName as key to mapper.
				config.set(hdfsInputPath.toUri().getPath(), type.getName());
			}

			config.setBoolean(ConfigUtil.CONF_IS_MULTITYPE, true);
			config.set(ConfigUtil.CONF_INDEX_CONTEXT, indexContext.toString());
			//这个在 HdfsSyncingLocalFileOutputCommitter 中会用到，且无法传入indexContext
			config.setInt(JobHelper.CONF_KEY_SHARD_NUM, indexContext.getNumberOfShards());

			numberOfShards = indexContext.getNumberOfShards();
			esIndicesPath = indexContext.getEsIndicesPath();
			esAddress = indexContext.getEsAddress();

		} else {
			jobHelper = JobHelper.parseFromArgs(args);
			config.set(JobHelper.CONF_KEY_TYPE, jobHelper.getTypeName());
			Path hdfsInputPath = ensureOnHdfs(jobHelper.getDataFilePath(), hdfs);
			inputPaths.add(hdfsInputPath);

			if (null != jobHelper.getRoutingField())
				config.set(JobHelper.CONF_KEY_ROUTING, jobHelper.getRoutingField());

			if (null != jobHelper.getTypeMapping())
				config.set(JobHelper.CONF_KEY_MAPPING, jobHelper.getTypeMapping());

			if (null != jobHelper.getPartitionFields())
				config.setStrings(JobHelper.CONF_KEY_PARTITION, jobHelper.getPartitionFields());

			config.set(JobHelper.CONF_KEY_INDEX, jobHelper.getIndexName());
			config.setStrings(JobHelper.CONF_KEY_FIELDS, jobHelper.getFieldNames());
			config.setInt(JobHelper.CONF_KEY_SHARD_NUM, jobHelper.getNumberOfShards());
			config.setInt(JobHelper.CONF_KEY_FIELD_SPLITTER, jobHelper.getFieldSplitter());

			numberOfShards = jobHelper.getNumberOfShards();
			esIndicesPath = jobHelper.getEsIndicesPath();
			esAddress = jobHelper.getEsAddress();
		}


		// IMPORTANT!!! this can decrease the io from map to reduce and improve the performance significantly
		// compress map output
		config.setBoolean("mapreduce.map.output.compress", true);
		config.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);

		// more reducer fetcher (default is 5)
		config.setInt("mapreduce.reduce.shuffle.parallelcopies", 10);

		// default is 4096
		config.setInt("io.file.buffer.size", 64 * 1024);

		config.setInt("mapreduce.reduce.cpu.vcores", 8);
		config.setInt("mapreduce.reduce.memory.mb", 8192);
		config.set("mapreduce.reduce.java.opts", "-Xmx8192M -XX:-UseGCOverheadLimit");


		config.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, 3);

		Job job = Job.getInstance(config, "es-offline-index");
		job.setJarByClass(JobMain.class);
		job.setMapperClass(OfflineIndexMapper.class);
		job.setReducerClass(OfflineIndexReducer.class);
		job.setOutputFormatClass(HdfsSyncingLocalFileOutputFormat.class);
		job.setInputFormatClass(KeyConfigedTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(numberOfShards);

		for(Path inputPath : inputPaths) {
			FileInputFormat.addInputPath(job, inputPath);
		}
		setLibs(job, hdfs);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

		while(!copyHelper.isCompleted()){
			logger.info("copying input file to hdfs...");
			Thread.sleep(2000);
		}
		copyHelper.destroy();

		if (!job.waitForCompletion(true)) {
			logger.info(job.getJobID() + " failed!! check http://hopc.hiido.net:19888/jobhistory/job/" + job.getJobID() + " for more information");
			System.exit(1);
		}

		logger.info("start mounting indices data to es...");
		new IndexSynchronizer(esIndicesPath, OUTPUT_DIR, hdfs, FileSystem.getLocal(config), esAddress).sync();

		System.exit(0);
	}


	private static void logArgs(String[] args) {
		logger.info("===================args=====================");
		for (String arg : args)
			logger.info(arg);
		logger.info("============================================");
	}


	private static Path ensureOnHdfs(URL input, FileSystem hdfs) throws IOException {
		if (!input.getProtocol().equals("hdfs")) {
			String inputPathStr = TMP_IO_PREFIX + "input_" + System.nanoTime();
			hdfs.mkdirs(new Path(inputPathStr));
			String dstPath = inputPathStr + File.separator + Utils.getFileNameFromPath(input.getPath());
			copyHelper.copy(new Path(input.toExternalForm()), new Path(dstPath));
			logger.info("copy local file: " + input.toExternalForm() + " to hdfs: " + dstPath);
			return new Path(dstPath);
		}
		return new Path(input.getPath());
	}

	private static void setLibs(Job job, FileSystem hdfs) throws IOException {
		URL libDirPath = JobMain.class.getClassLoader().getResource("lib");
		if (null == libDirPath)
			throw new OfflineIndexException("cannot find libs dir");
		File dir = new File(libDirPath.getPath());
		for (File lib : dir.listFiles()) {
			String libName = lib.getName();
			Path hdfsPath = new Path(JobHelper.HDFS_LIB_DIR_PATH + libName);
			if (!hdfs.exists(hdfsPath)) {
				logger.info("copy local lib:" + lib.getPath() + " to hdfs: " + hdfsPath.toUri().getPath());
				hdfs.copyFromLocalFile(new Path(lib.getPath()), hdfsPath);
			}
			job.addFileToClassPath(hdfsPath);
		}
	}
}
