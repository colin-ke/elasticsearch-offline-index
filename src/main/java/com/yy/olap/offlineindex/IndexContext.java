package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.exceptions.ConfigException;
import com.yy.olap.offlineindex.exceptions.OfflineIndexException;
import com.yy.olap.offlineindex.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

import static com.yy.olap.offlineindex.utils.Utils.getNotNullFromProp;

/**
 * @author colin.ke keqinwu@163.com
 */
public class IndexContext {

	private static final String KEY_NAME = "indexName";
	private static final String KEY_INDICES_PATH = "esIndicesPath";
	private static final String KEY_ES_ADDRESS = "esAddress";
	private static final String KEY_PARTITION = "partitionFields";
	private static final String KEY_SHARDS = "numOfShards";
	private static final String KEY_TYPES = "types";

	private String name;
	private String esIndicesPath;
	private String esAddress;
	private int numberOfShards;
	private List<TypeContext> types = new ArrayList<>();
	//optional
	private String[] partitionFields;




	public static IndexContext fromProperties(String propLocation) throws IOException {
		Properties properties = new Properties();
		properties.load(new BufferedInputStream(new FileInputStream(propLocation)));

		IndexContext index = new IndexContext();
		index.setName(getNotNullFromProp(properties, KEY_NAME));
		index.setEsAddress(getNotNullFromProp(properties, KEY_ES_ADDRESS));
		index.setEsIndicesPath(getNotNullFromProp(properties, KEY_INDICES_PATH));
		index.setNumberOfShards(Integer.parseInt(getNotNullFromProp(properties, KEY_SHARDS)));
		if (properties.containsKey(KEY_PARTITION))
			index.setPartitionFields(Utils.split(properties.getProperty(KEY_PARTITION), ','));

		index.types = TypeContext.fromProperties(properties);

		return index;
	}

	public String toProperties() throws IOException {
		Properties properties = new Properties();
		properties.put(KEY_NAME, name);
		properties.put(KEY_ES_ADDRESS, esAddress);
		properties.put(KEY_INDICES_PATH, esIndicesPath);
		properties.put(KEY_SHARDS, String.valueOf(numberOfShards));
		if (null != partitionFields)
			properties.put(KEY_PARTITION, Utils.join(partitionFields, ','));
		for (int i = 0; i < types.size(); i++) {
			Map<String, String> type = types.get(i).toMap();
			for (String key : type.keySet()) {
				properties.put("type" + (i + 1) + "." + key, type.get(key));
			}
		}

		String path = Utils.getTmpDirPath() + "offline-index" + System.currentTimeMillis() + ".properties";
		File file = new File(path);
		if (!file.createNewFile()) {
			throw new OfflineIndexException("could not create properties file: " + path);
		}
		properties.store(new BufferedOutputStream(new FileOutputStream(file)), "");

		return path;
	}


	public static IndexContext fromString(String jsonString) {
		JSONObject obj = new JSONObject(jsonString);
		IndexContext index = new IndexContext();
		index.setName(obj.getString(KEY_NAME));
		index.setNumberOfShards(obj.getInt(KEY_SHARDS));
		index.setEsAddress(obj.getString(KEY_ES_ADDRESS));
		index.setEsIndicesPath(obj.getString(KEY_INDICES_PATH));
		if (obj.has(KEY_PARTITION))
			index.setPartitionFields(Utils.split(obj.getString(KEY_PARTITION), ','));

		for (Object typeStr : obj.getJSONArray(KEY_TYPES)) {
			index.types.add(TypeContext.fromString(typeStr.toString()));
		}

		return index;

	}

	@Override
	public String toString() {
		JSONObject obj = new JSONObject();
		obj.put(KEY_NAME, getName());
		obj.put(KEY_SHARDS, getNumberOfShards());
		obj.put(KEY_ES_ADDRESS, getEsAddress());
		obj.put(KEY_INDICES_PATH, getEsIndicesPath());
		JSONArray typeArray = new JSONArray();
		for(TypeContext type : types)
			typeArray.put(type.toMap());
		obj.put(KEY_TYPES, typeArray);

		if (null != getPartitionFields())
			obj.put(KEY_PARTITION, Utils.join(getPartitionFields(), ','));

		return obj.toString();
	}

	Map<String, Integer[]> pKeyIndexes = new HashMap<>();
	public Integer[] getPkeyIndexes(String type) {
		if(pKeyIndexes.containsKey(type))
			return pKeyIndexes.get(type);

		for(TypeContext t : types) {
			if(t.getName().equals(type)) {
				Integer[] arr = new Integer[partitionFields.length];
				for (int i = 0; i < arr.length; ++i) {
					int k = Utils.indexOf(t.getFieldNames(), partitionFields[i]);
					if (-1 == k)
						throw new ConfigException("cannot find the partition key: " + partitionFields[i] + " in the given fieldNames:" + Utils.join(t.getFieldNames(), ','));
					arr[i] = k;
				}
				pKeyIndexes.put(type, arr);
				return arr;
			}
		}
		return null;
	}

	public void addTypeContext(TypeContext typeContext) {
		types.add(typeContext);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEsIndicesPath() {
		return esIndicesPath;
	}

	public void setEsIndicesPath(String esIndicesPath) {
		this.esIndicesPath = esIndicesPath;
	}

	public String getEsAddress() {
		return esAddress;
	}

	public void setEsAddress(String esAddress) {
		this.esAddress = esAddress;
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}

	public void setNumberOfShards(int numberOfShards) {
		this.numberOfShards = numberOfShards;
	}

	public List<TypeContext> getTypes() {
		return types;
	}

	public void setTypes(List<TypeContext> types) {
		this.types = types;
	}

	public String[] getPartitionFields() {
		return partitionFields;
	}

	public void setPartitionFields(String[] partitionFields) {
		this.partitionFields = partitionFields;
	}
}
