package com.yy.olap.offlineindex.exceptions;

/**
 * @author colin.ke keqinwu@163.com
 */
public class OfflineIndexException extends RuntimeException {
	public OfflineIndexException(String msg) {
		super(msg);
	}

	public OfflineIndexException(Exception e) {
		super(e);
	}

	public OfflineIndexException(String msg, Exception e) {
		super(msg, e);
	}
}
