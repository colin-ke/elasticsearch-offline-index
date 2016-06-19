package com.yy.olap.offlineindex.exceptions;

/**
 * @author colin.ke keqinwu@163.com
 */
public class IndexSyncException extends RuntimeException {
	public IndexSyncException(String msg) {
		super(msg);
	}

	public IndexSyncException(Exception e) {
		super(e);
	}

	public IndexSyncException(String msg, Exception e) {
		super(msg, e);
	}
}
