package com.yy.olap.offlineindex.output;

import com.yy.olap.offlineindex.IndexContext;
import com.yy.olap.offlineindex.JobHelper;
import com.yy.olap.offlineindex.OfflineIndexMapper;
import com.yy.olap.offlineindex.utils.EsUtils;
import com.yy.olap.offlineindex.utils.MultiThreadCopyHelper;
import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author colin.ke keqinwu@163.com
 */
public class HdfsSyncingLocalFileOutputCommitter extends FileOutputCommitter {

	private static Log logger = LogFactory.getLog("OUTPUT_COMMITTER");

//	public static final String PREFIX_LUCENE_INDEX_PART = "shard-";
	public static final String SHARD_STATE_DIR_NAME = "_state";
	public static final String INDEX_STATE_DIR_NAME = "_state";
	public static final String SHARD_TRANSLOG_DIR_NAME = "translog";

	private final FileSystem localFileSystem;
	//	private final File localScratchPath;
	private final String localPathBase;
	private final Map<String, File> localShardFolders;
	private final Map<String, Set<String>> emptyShards;

	private final FileSystem hdfsFileSystem;
	private final Path hdfsOutputPath;

	private static Map<String, Map<String, String>> indexMappings = new HashMap<>();

	public HdfsSyncingLocalFileOutputCommitter(String localShardFolderBase, Path hdfsOutputPath, TaskAttemptContext context) throws IOException {
		super(hdfsOutputPath, context);

		Configuration conf = context.getConfiguration();

		this.localFileSystem = FileSystem.getLocal(conf);
		if (localShardFolderBase.endsWith(File.separator))
			this.localPathBase = localShardFolderBase;
		else
			this.localPathBase = localShardFolderBase + File.separator;
		localShardFolders = new HashMap<>();
		emptyShards = new HashMap<>();

		this.hdfsFileSystem = FileSystem.get(conf);
		this.hdfsOutputPath = hdfsOutputPath;
//		this.hdfsOutputPath = new Path("/tmp/offline_index/output");
//		hdfsShardFolders = new HashMap<>();
	}

	public static void setIndexMapping(String index, String type, String mapping) {
		Map<String, String> typeMappings = indexMappings.get(index);
		if(null == typeMappings) {
			typeMappings = new HashMap<>();
			indexMappings.put(index, typeMappings);
		}
		typeMappings.put(type, mapping);
	}

	public void noteEmptyShard(String index, String shard) {
		Set<String> shardIdSet = emptyShards.get(index);
		if(null == shardIdSet) {
			shardIdSet = new HashSet<>();
			emptyShards.put(index, shardIdSet);
		}
		shardIdSet.add(shard);
	}

	public File getLocalScratchPath(String indexShard, TaskAttemptContext context) {
		File dir = localShardFolders.get(indexShard);
		if (null == dir) {
			dir = new File(localPathBase + getShardFolderName(indexShard, context));
			if (dir.exists()) {
				throw new RuntimeException("local shard folder " + dir.getPath() + " already exists");
			}
			if (!dir.mkdir()) {
				throw new RuntimeException("cannot create local shard folder: " + dir.getPath());
			}
			localShardFolders.put(indexShard, dir);
		}
		return dir;
	}

	@Override
	public void abortJob(JobContext context, JobStatus.State state) throws IOException {
		deleteLocalScratchPath();
		super.abortJob(context, state);
	}

	@Override
	public void abortTask(TaskAttemptContext context) throws IOException {
		deleteLocalScratchPath();
		super.abortTask(context);
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {
		syncToHdfs(context);
		super.commitTask(context);
		deleteLocalScratchPath();
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
		return !localShardFolders.isEmpty() || super.needsTaskCommit(context);
	}

	private void syncToHdfs(TaskAttemptContext context) {
		MultiThreadCopyHelper copyHelper = new MultiThreadCopyHelper(localFileSystem,hdfsFileSystem, 15, MultiThreadCopyHelper.CopyType.UPLOAD);
		long start = System.currentTimeMillis();
		try {
			logger.info("start committing index data to hdfs...");
			logger.info("local shard folder count: " + localShardFolders.size());
			if (!hdfsFileSystem.exists(hdfsOutputPath) && !hdfsFileSystem.mkdirs(hdfsOutputPath)) {
				throw new IOException(String.format("Cannot create HDFS directory at [%s] to sync Lucene index!", hdfsOutputPath));
			}
			String outputPathStr = hdfsOutputPath.toUri().getPath();

			Map<String, List<File>> indexShards = new HashMap<>();
			for (String compoundName : localShardFolders.keySet()) {
				//TODO check ex
				String index = Utils.split(compoundName, OfflineIndexMapper.KEY_SEPARATOR)[0];
				List<File> shardFiles = indexShards.get(index);
				if (null == shardFiles) {
					shardFiles = new ArrayList<>();
					indexShards.put(index, shardFiles);
				}
				shardFiles.add(localShardFolders.get(compoundName));
			}

			for (String index : indexShards.keySet()) {
				boolean needToWriteIndexState = false;
				Set<String> emptyShard = emptyShards.get(index);
				if(null == emptyShard)
					emptyShard = Collections.emptySet();
				for (File shardDir : indexShards.get(index)) {
					//TODO check ex
					String shardNum = Utils.split(shardDir.getName(), OfflineIndexMapper.KEY_SEPARATOR)[1];

					if(shardNum.equals("0")) {
						// 每个索引的0号shard负责写索引元数据
						needToWriteIndexState = true;
					}

					logger.info("committing index:" + index + ", shardNum:" + shardNum);
					// {outputPath}/{indexName}/{shardNum}/
					String hdfsShardPathStr = Utils.ensureEndWithFileSeparator(outputPathStr) + index + File.separator + shardNum + File.separator;
					hdfsFileSystem.mkdirs(new Path(hdfsShardPathStr));

					//1.copy index data 2.create shard _state dir 3.create shard translog dir
					if(emptyShard.contains(shardNum)) {
						hdfsFileSystem.mkdirs(new Path(hdfsShardPathStr + "index"));
						hdfsFileSystem.createNewFile(new Path(hdfsShardPathStr + "index/" + "write.lock"));
					} else {
						copyHelper.copy(new Path(shardDir.getPath()), new Path(hdfsShardPathStr + "index"));
					}
					hdfsFileSystem.mkdirs(new Path(hdfsShardPathStr + SHARD_STATE_DIR_NAME));
					hdfsFileSystem.mkdirs(new Path(hdfsShardPathStr + SHARD_TRANSLOG_DIR_NAME));

					//copy shard state file;
					//TODO should not hard-code
					String localStateTmplFileName = "state-2";
					String stateFilePathStr = Utils.copyResourceToLocalTmp(localStateTmplFileName);
					copyHelper.copy(new Path(stateFilePathStr),
							new Path(hdfsShardPathStr + SHARD_STATE_DIR_NAME + File.separator + localStateTmplFileName));
				}

				// write index meta file(state-X)
				if (needToWriteIndexState) {
					String hdfsStateDirPathStr = Utils.ensureEndWithFileSeparator(outputPathStr) + index + File.separator + INDEX_STATE_DIR_NAME + File.separator;
					int shards = context.getConfiguration().getInt(JobHelper.CONF_KEY_SHARD_NUM, 5);
					String indexStateFilePath = EsUtils.genIndexMetaFile(index, shards, 1, 2, indexMappings.get(index));

					// create index state dir
					hdfsFileSystem.mkdirs(new Path(hdfsStateDirPathStr));
					hdfsFileSystem.copyFromLocalFile(new Path(indexStateFilePath),
							new Path(hdfsStateDirPathStr + File.separator + Utils.getFileNameFromPath(indexStateFilePath)));
				}
			}
			while(!copyHelper.isCompleted()){
				logger.info("committing data to hdfs... left: " + copyHelper.getCopyingCount());
				Thread.sleep(2000);
			}
			logger.info("committing successfully! total took:" + (System.currentTimeMillis() - start) + "ms");
		} catch (Exception e){
			logger.error("", e);
		} finally {
			copyHelper.destroy();
		}


//		for (String shard : localShardFolders.keySet()) {
//
//			// Create subdirectory in HDFS for the Lucene index part from this particular reducer.
//			Path indexPartHdfsFilePath = new Path(hdfsOutputPath, getShardFolderName(shard, context));
//
//			if (!hdfsFileSystem.mkdirs(indexPartHdfsFilePath)) {
//				throw new IOException(String.format("Cannot create HDFS directory at [%s] to sync Lucene index!", indexPartHdfsFilePath));
//			}
//
//			for (File localFile : localShardFolders.get(shard).listFiles()) {
//				context.progress();
//
//				Path localFilePath = new Path("file://" + localFile.getPath());
//
//				if (!localFileSystem.exists(localFilePath)) {
//					throw new IOException(String.format("Cannot find local file [%s]!", localFilePath));
//				}
//
//				Path hdfsFilePath = new Path(indexPartHdfsFilePath, localFile.getName());
//				if (hdfsFileSystem.exists(hdfsFilePath)) {
//					throw new IOException(String.format("HDFS file [%s] already exists!", hdfsFilePath));
//				}
//
//				copyHelper.copy(localFilePath, hdfsFilePath);
//			}
//		}
	}


	private String getShardFolderName(String indexShard, TaskAttemptContext context) {
//		return PREFIX_LUCENE_INDEX_PART + indexShard + "_part-" + context.getTaskAttemptID().getTaskID().getId();
		return indexShard;
	}

	private void deleteLocalScratchPath() {
		try {
			for (File dir : localShardFolders.values()) {
				FileUtils.deleteDirectory(dir);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
