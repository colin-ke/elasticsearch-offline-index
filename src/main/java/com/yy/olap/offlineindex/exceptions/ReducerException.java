package com.yy.olap.offlineindex.exceptions;

/**
 * @author colin.ke keqinwu@163.com
 */
public class ReducerException extends RuntimeException {
	public ReducerException(String msg) {
		super(msg);
	}

	public ReducerException(Exception e) {
		super(e);
	}

	public ReducerException(String msg, Exception e) {
		super(msg, e);
	}
}
