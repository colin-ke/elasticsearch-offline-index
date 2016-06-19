package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.exec.DefaultExecuteResultHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author colin.ke keqinwu@163.com
 */
public class JobFuture implements Future {

	DefaultExecuteResultHandler resultHandler;
	String outFilePath;
	String jobId;
	String Index;
	List<String> type;

	public JobFuture(DefaultExecuteResultHandler resultHandler, String outFilePath) {
		this.resultHandler = resultHandler;
		this.outFilePath = outFilePath;
		type = new ArrayList<>();
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return resultHandler.hasResult();
	}

	@Override
	public Object get() throws InterruptedException, ExecutionException {
		resultHandler.waitFor();
		return this;
	}

	@Override
	public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long timeoutInMillis;
		if (null != unit)
			timeoutInMillis = unit.toMillis(timeout);
		else
			timeoutInMillis = timeout;
		resultHandler.waitFor(timeoutInMillis);
		return this;
	}

	public String getOutFilePath() {
		return outFilePath;
	}

	public String getJobId() {
		if (null != jobId)
			return jobId;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(outFilePath));
			String line = reader.readLine();
			while (null != line) {
				if (line.contains("Submitting tokens for job:")) {
					jobId = line.substring(line.lastIndexOf(' ') + 1, line.length());
					return jobId;
				}
				line = reader.readLine();
			}
		} catch (IOException ignored) {

		} finally {
			if (null != reader)
				try {
					reader.close();
				} catch (IOException ignored) {
				}
		}
		return "";
	}

	public String getIndex() {
		return Index;
	}

	public void setIndex(String index) {
		Index = index;
	}

	public List<String> getType() {
		return type;
	}

	public void addType(String type) {
		this.type.add(type);
	}

	public void addTypes(List<String> types) {
		this.type.addAll(types);
	}
}
