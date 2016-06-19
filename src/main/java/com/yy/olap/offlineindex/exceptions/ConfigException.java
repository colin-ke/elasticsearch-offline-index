package com.yy.olap.offlineindex.exceptions;

/**
 * @author colin.ke keqinwu@163.com
 */
public class ConfigException extends RuntimeException {
	public ConfigException(String msg) {
		super(msg);
	}

	public ConfigException(Exception e) {
		super(e);
	}

	public ConfigException(String msg, Exception e) {
		super(msg, e);
	}
}
