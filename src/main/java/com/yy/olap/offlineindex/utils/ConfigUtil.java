package com.yy.olap.offlineindex.utils;

/**
 * @author colin.ke keqinwu@163.com
 */
public class ConfigUtil {

	public static final String CONF_IS_MULTITYPE = "job.isMultiType";
	public static final String CONF_INDEX_CONTEXT = "job.indexContext";

	public static String getTypeInputConfKey(String type) {
		return type + "_INPUT";
	}
}
