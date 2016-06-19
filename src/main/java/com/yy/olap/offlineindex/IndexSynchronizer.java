package com.yy.olap.offlineindex;

import com.yy.elasticsearch.dangledindices.action.AllocateDangledIndicesAction;
import com.yy.elasticsearch.dangledindices.action.AllocateDangledIndicesResponse;
import com.yy.olap.offlineindex.exceptions.IndexSyncException;
import com.yy.olap.offlineindex.utils.EsUtils;
import com.yy.olap.offlineindex.utils.MultiThreadCopyHelper;
import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.indices.IndexMissingException;

import java.util.ArrayList;
import java.util.List;


/**
 * @author colin.ke keqinwu@163.com
 */
public class IndexSynchronizer {

	private static Log logger = LogFactory.getLog("INDEX_SYNCHRONIZER");
	private static final String JOB_STATUS_FILE_NAME = "_SUCCESS";

	private String esIndicesDir;
	private String hdfsOutputDir;
	private FileSystem hdfs;
	private FileSystem localFs;
	private Client esClient;

	private final int RETRY_TIMES = 15;
	private int newIndexReplicaNum = 1;

	private PathFilter pathFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return !path.getName().equals(JOB_STATUS_FILE_NAME);
		}
	};

	/**
	 * constructor of IndexSynchronizer
	 *
	 * @param esIndicesDir  es cluster's data dir path
	 * @param hdfsOutputDir the offline-index mr job's output dir
	 * @param hdfs          hdfs file system
	 * @param esAddress     es cluster's client address, format: {HOST}:{PORT}
	 */
	public IndexSynchronizer(String esIndicesDir, String hdfsOutputDir, FileSystem hdfs, FileSystem localFs, String esAddress) {
		this.esIndicesDir = Utils.ensureEndWithFileSeparator(esIndicesDir);
		this.hdfsOutputDir = Utils.ensureEndWithFileSeparator(hdfsOutputDir);
		this.hdfs = hdfs;
		this.localFs = localFs;
		this.esClient = EsUtils.getClient(esAddress);
	}

	public void sync() {
		MultiThreadCopyHelper copyHelper = new MultiThreadCopyHelper(hdfs, localFs, 5, MultiThreadCopyHelper.CopyType.DOWNLOAD);
		try {
			List<String> indices = new ArrayList<>();
			Iterable<FileStatus> fsIt = new LocatedFileStatusFetcher(hdfs.getConf(), new Path[]{new Path(hdfsOutputDir)}, false, pathFilter, true).getFileStatuses();
			for (FileStatus fileStatus : fsIt) {
				String fileName = Utils.getFileNameFromPath(fileStatus.getPath().toUri().getPath());
//				if (fileName.equals(JOB_STATUS_FILE_NAME))
//					continue;
				//fileName 就是indexName
				if (isIndexExist(fileName)) {
//					logger.info("index:" + fileName + " already exists, currently only support mounting new indices");
//					continue;
					logger.info("index:" + fileName + " already exists, will be deleted");
					deleteIndex(fileName);
				}
				indices.add(fileName);
				copyHelper.copy(fileStatus.getPath(), new Path(esIndicesDir + fileName));
				logger.info("copying " + fileStatus.getPath().toUri().getPath() + " to " + esIndicesDir + fileName);
			}

			while (!copyHelper.isCompleted()) {
				logger.info("copying index data... left:" + copyHelper.getCopyingCount());
				Thread.sleep(2000);
			}

			if (indices.isEmpty())
				return;
			// trigger clusterChanged event so that es will scan all the indices under the data folder
			for (int i = 0; i < RETRY_TIMES; ++i) {
				try {
					logger.info("start triggering allocation of indices: " + indices);
					AllocateDangledIndicesResponse res = esClient.admin().indices().prepareExecute(AllocateDangledIndicesAction.INSTANCE).setIndices(indices.toArray(new String[indices.size()])).execute().actionGet();
					if (res.isAcknowledged()) {
						logger.info("succeed allocating dangled indices");
						return;
					} else {
						logger.error("failed to allocate dangled indices");
					}
				} catch (Exception e) {
					logger.error("ex happen while triggering allocation", e);
				}
				logger.info("retry...");
				Thread.sleep(getRetrySleepTimeInMilli(i));
			}
			logger.info("out of retry times:" + RETRY_TIMES);
		} catch (Exception e) {
			throw new IndexSyncException(e);
		} finally {
			copyHelper.destroy();
		}

	}

	private boolean isIndexExist(String indexName) throws InterruptedException {
		Exception ex = null;
		for (int i = 0; i < RETRY_TIMES; ++i) {
			try {
				return esClient.admin().indices().prepareExists(indexName).execute().get().isExists();
			} catch (Exception e) {
				ex = e;
				if (i < RETRY_TIMES - 1)
					Thread.sleep(getRetrySleepTimeInMilli(i));
				logger.error("error when check index existence: ", e);
				logger.info("retrying...");
			}
		}
		logger.info("out of retry times:" + RETRY_TIMES);
		throw new IndexSyncException("failed to check index existence: " + ex.getMessage());
	}

	private void deleteIndex(String index) throws InterruptedException {
		logger.debug("start deleting index: " + index);
		while (true) {
			try {
				DeleteIndexResponse res = esClient.admin().indices().prepareDelete(index).execute().actionGet();
				if (res.isAcknowledged())
					break;

				logger.warn("delete index action is not acknowledged:" + index + " retrying");
			} catch (ProcessClusterEventTimeoutException e) {
				// 删索引时经常爆这个错~无视
			} catch (IndexMissingException e) {
				break;
			}

			Thread.sleep(5000);
		}
		logger.debug("successfully delete index: " + index);
	}

	private long getRetrySleepTimeInMilli(int time) {
		return 100 * (1 << time);
	}

	public int getNewIndexReplicaNum() {
		return newIndexReplicaNum;
	}

	public void setNewIndexReplicaNum(int newIndexReplicaNum) {
		this.newIndexReplicaNum = newIndexReplicaNum;
	}

}
