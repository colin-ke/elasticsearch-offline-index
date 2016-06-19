package com.yy.olap.offlineindex.utils;

import com.yy.olap.offlineindex.exceptions.OfflineIndexException;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.elasticsearch.common.jackson.core.io.JsonStringEncoder;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author colin.ke keqinwu@163.com
 */
public class Utils {

	private static String tmpDir = ensureEndWithFileSeparator(System.getProperty("java.io.tmpdir")) + "offline_index_" + System.nanoTime() + File.separator;
	private static final String HIVE_SPLITTER = "u0001";
	private static boolean hasRegisteredURLStreamHandler = false;

	/**
	 * Pattern that matches any string
	 */
	public static final Pattern MATCH_ANY = Pattern.compile(".*");
	public static final String MAPSTR_PARAM_PATTERN = "(.+:.+,?)+";

	static {
		new File(tmpDir).mkdir();
	}

	public static String getTmpDirPath() {
		return tmpDir;
	}

	public static String join(String[] array, char separator) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < array.length - 1; ++i) {
			sb.append(array[i]).append(separator);
		}
		sb.append(array[array.length - 1]);
		return sb.toString();
	}

	public static String[] split(String str, char separator) {
		List<String> res = new ArrayList<>();
		char[] chs = str.toCharArray();
		int i = 0, j = 0;
		if(chs[0] == separator) {
			res.add("");
			++i;
		}
		for (; j < chs.length; ++j) {
			if (chs[j] == separator && j > 0 && chs[j - 1] != '\\') {
				res.add(str.substring(i, j));
				i = j + 1;
			}
		}
		res.add(str.substring(i, j));
		return res.toArray(new String[res.size()]);
	}

	private static JsonStringEncoder jsonStringEncoder = new JsonStringEncoder();
	public static String toJson(String[] line, String[] fieldNames, String type) {
		StringBuilder sb = new StringBuilder("{");
		if (line.length != fieldNames.length) {
			throw new OfflineIndexException("line's len should be the same as the fieldNames' length");
		}
		String val;
		for (int i = 0; i < line.length; ++i) {
			if (line[i].isEmpty() || line[i].equals("\\N"))
				continue;

			if (line[i].indexOf('\\') > 0)
				val = line[i].replace("\\", "");
			else
				val = line[i];
			sb.append("\"").append(jsonStringEncoder.quoteAsString(fieldNames[i])).append("\"").append(":");
			sb.append("\"").append(jsonStringEncoder.quoteAsString(val)).append("\"").append(",");
		}

		sb.append("\"").append("_id").append("\"").append(":");
		sb.append("\"").append(EsUtils.randomBase64UUID()).append("\"");
		sb.append("}");
		return sb.toString();
	}

	public static int indexOf(String[] array, String strToFind) {
		for (int i = 0; i < array.length; ++i)
			if (array[i].equals(strToFind))
				return i;

		return -1;
	}


	public static String ensureEndWithFileSeparator(String path) {
		if (path.endsWith(File.separator))
			return path;
		else
			return path + File.separator;
	}

	public static String getFileNameFromPath(String path) {
		return path.substring(path.lastIndexOf(File.separatorChar) + 1, path.length());
	}

	public static String copyResourceToLocalTmp(String resourceName) throws IOException {
		InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(resourceName);
		if (null == inputStream)
			return null;
		String resTmpPath = tmpDir + resourceName + "_" + System.nanoTime();
		if (!new File(resTmpPath).createNewFile())
			throw new OfflineIndexException("couldn't create tmp file: " + resTmpPath);
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(resTmpPath));
		byte[] buffer = new byte[1024];
		int readCount = inputStream.read(buffer);
		while (readCount > 0) {
			out.write(buffer, 0, readCount);
			readCount = inputStream.read(buffer);
		}

		inputStream.close();
		out.flush();
		out.close();

		return resTmpPath;
	}

	public static String MapToStr(Map<String, String> map){
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, String> entry : map.entrySet()) {
			if(sb.length() > 0)
				sb.append(',');
			sb.append(entry.getKey()).append(':').append(entry.getValue());
		}
		return sb.toString();
	}

	public static Map<String, String> StrToMap(String str) {
		if(!str.matches(MAPSTR_PARAM_PATTERN))
			return null;
		Map<String, String> map = new HashMap<>();
		for(String compoundStr : Utils.split(str, ',')) {
			String[] pair = Utils.split(compoundStr, ':');
			map.put(pair[0], pair[1]);
		}
		return map;
	}

	public static String splitterToStr(char splitter) {
		if(splitter == '\01')
			return "u0001";
		return String.valueOf(splitter);
	}

	public static char strToSplitter(String splitter) {
		return splitter.equals(HIVE_SPLITTER) ? '\01' : splitter.charAt(0);
	}

	public static String getNotNullFromProp(Properties properties, String key){
		if(!properties.containsKey(key))
			throw new OfflineIndexException("缺少参数：" + key);
		return properties.getProperty(key);
	}

	public static void registeredURLStreamHandler() {
		if(hasRegisteredURLStreamHandler)
			return;
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		hasRegisteredURLStreamHandler = true;
	}

	private static final String URL_PATTERN = ".+://.+";
	public static String ensureURLPattern(String urlStr){
		if(!urlStr.matches(URL_PATTERN))
			return "file://" + urlStr;
		return urlStr;
	}

}
