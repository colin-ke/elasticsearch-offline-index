package com.yy.olap.offlineindex.utils;

import com.yy.olap.offlineindex.exceptions.OfflineIndexException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author colin.ke keqinwu@163.com
 */
public class MultiThreadCopyHelper {

	static private Log logger = LogFactory.getLog(MultiThreadCopyHelper.class.getName());

	public enum CopyType {
		UPLOAD, DOWNLOAD
	}

	private ExecutorService threadPool;
	private FileSystem srcFs;
	private FileSystem dstFs;
	private AtomicLong copyingCount = new AtomicLong(0);
	private boolean listFinished = true;
	private CopyType type;
	private List<Exception> exceptions = new ArrayList<>();

	public MultiThreadCopyHelper(FileSystem srcFs, FileSystem local, int threadCount, CopyType type) {
		threadPool = Executors.newFixedThreadPool(threadCount);
		this.srcFs = srcFs;
		this.dstFs = local;
		this.type = type;
	}

	public void copy(Path src, Path dst) throws IOException {
		listFinished = false;
		try {
			if (srcFs.isFile(src))
				copyFile(src, dst);
			else
				copyDir(src, dst);
		} finally {
			listFinished = true;
		}
	}

	private void copyDir(Path src, Path dst) throws IOException {
		copyingCount.incrementAndGet();
		threadPool.submit(new CopyDirTask(src, dst));
	}

	private void copyFile(Path src, Path dst) {
		copyingCount.incrementAndGet();
		threadPool.submit(new CopyFileTask(src, dst));
	}

	public boolean isCompleted() {
		return listFinished && copyingCount.get() == 0;
	}

	public long getCopyingCount() {
		return copyingCount.get();
	}

	public List<Exception> getExceptions() {
		return exceptions;
	}

	public void destroy() {
		threadPool.shutdown();
	}

	class CopyDirTask implements Callable<Void> {

		private Path src;
		private Path dst;

		CopyDirTask(Path src, Path dst) {
			this.src = src;
			this.dst = dst;
		}

		@Override
		public Void call() throws Exception {
			doCopy();
			return null;
		}

		private void doCopy() {
			try {
				if (!dstFs.exists(dst) && !dstFs.mkdirs(dst)) {
					logger.error("could not create dir: " + dst.toUri().toURL().toExternalForm());
					System.out.println("could not create dir: " + dst.toUri().toURL().toExternalForm());
					exceptions.add(new OfflineIndexException("could not create dir: " + dst.toUri().toURL().toExternalForm()));
					return;
				}

				for (FileStatus fs : srcFs.listStatus(src)) {
					if (fs.isDirectory()) {
						copyDir(fs.getPath(), new Path(dst, fs.getPath().getName()));
					} else {
						copyFile(fs.getPath(), new Path(dst, fs.getPath().getName()));
					}
				}
			} catch (Exception e) {
				logger.error("failed to copy dir: " + src + " to " + dst + " ex:" + e.getMessage());
				System.out.println("failed to copy dir: " + src + " to " + dst + " ex:" + e.getMessage());
			} finally {
				copyingCount.decrementAndGet();
			}
		}
	}

	class CopyFileTask implements Callable<Void> {

		private Path src;
		private Path dst;

		CopyFileTask(Path src, Path dst) {
			this.src = src;
			this.dst = dst;
		}

		@Override
		public Void call() throws Exception {
			try {
				if (type == CopyType.DOWNLOAD)
					srcFs.copyToLocalFile(src, dst);
				else if (type == CopyType.UPLOAD)
					dstFs.copyFromLocalFile(src, dst);
			} finally {
				copyingCount.decrementAndGet();
			}
			return null;
		}

//		private void doCopy() throws IOException {
//			OutputStream out = dstFs.create(dst);
//			InputStream in = srcFs.open(src);
//			if (type == CopyType.DOWNLOAD)
//				in = new GzipCodec().createInputStream(in);
//			else if (type == CopyType.UPLOAD)
//				out = new GzipCodec().createOutputStream(out);
//			try {
//				IOUtils.copyBytes(in, out, bufferSize, false);
//			} finally {
//				in.close();
//				out.close();
//			}
//		}
	}

}
