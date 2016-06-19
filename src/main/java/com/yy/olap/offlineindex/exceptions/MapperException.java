package com.yy.olap.offlineindex.exceptions;

/**
 * @author colin.ke keqinwu@163.com
 */
public class MapperException extends RuntimeException {

	public MapperException(String msg) {
		super(msg);
	}

	public MapperException(Exception e) {
		super(e);
	}

	public MapperException(String msg, Exception e) {
		super(msg, e);
	}
}
