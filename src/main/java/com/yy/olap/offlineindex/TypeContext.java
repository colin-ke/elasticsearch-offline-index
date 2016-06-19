package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.utils.Utils;
import org.json.JSONObject;

import java.util.*;

/**
 * @author colin.ke keqinwu@163.com
 */
public class TypeContext {

	private static final String KEY_NAME = "name";
	private static final String KEY_INPUTPATH = "inputPath";
	private static final String KEY_FIELDNAMES = "fieldNames";
	private static final String KEY_SPLITTER = "fieldSplitter";
	private static final String KEY_MAPPING = "mapping";
	private static final String KEY_ROUTING = "routingField";

	private static final String PROP_KEY_PATTERN = "type\\d+\\..+";

	private String name;
	private String inputPath;
	private String[] fieldNames;
	private char fieldSplitter;

	// optional
	private String mapping;
	private String routingField;


	public static List<TypeContext> fromProperties(Properties properties) {
		Map<String, Map<String, String>> typeParams = new HashMap<>();
		for (Object key : properties.keySet()) {
			if (key.toString().matches(PROP_KEY_PATTERN)) {
				String num = key.toString().substring("type".length(), key.toString().indexOf('.'));
				Map<String, String> paramSet = typeParams.get(num);
				if (null == paramSet) {
					paramSet = new HashMap<>();
					typeParams.put(num, paramSet);
				}
				paramSet.put(key.toString().substring(key.toString().indexOf('.') + 1), properties.getProperty(key.toString()));
			}
		}

		List<TypeContext> types = new ArrayList<>();
		for (String key : typeParams.keySet()) {
			Map<String, String> param = typeParams.get(key);
			TypeContext type = new TypeContext();
			type.setName(param.get(KEY_NAME));
			type.setInputPath(param.get(KEY_INPUTPATH));
			type.setFieldNames(Utils.split(param.get(KEY_FIELDNAMES), ','));
			type.setFieldSplitter(Utils.strToSplitter(param.get(KEY_SPLITTER)));

			if (param.containsKey(KEY_MAPPING))
				type.setMapping(param.get(KEY_MAPPING));
			if (param.containsKey(KEY_ROUTING))
				type.setRoutingField(param.get(KEY_ROUTING));

			types.add(type);
		}

		return types;
	}

	public Map<String, String> toMap() {
		Map<String, String> map = new HashMap<>();
		map.put(KEY_NAME, name);
		map.put(KEY_INPUTPATH, inputPath);
		map.put(KEY_FIELDNAMES, Utils.join(fieldNames, ','));
		map.put(KEY_SPLITTER, Utils.splitterToStr(fieldSplitter));

		if (null != mapping)
			map.put(KEY_MAPPING, mapping);

		if (null != routingField) {
			map.put(KEY_ROUTING, routingField);
		}

		return map;
	}

	public static TypeContext fromString(String jsonStr) {
		JSONObject object = new JSONObject(jsonStr);
		TypeContext type = new TypeContext();
		type.setName(object.getString(KEY_NAME));
		type.setInputPath(object.getString(KEY_INPUTPATH));
		type.setFieldNames(Utils.split(object.getString(KEY_FIELDNAMES), ','));
		type.setFieldSplitter(Utils.strToSplitter(object.getString(KEY_SPLITTER)));
		if (object.has(KEY_MAPPING))
			type.setMapping(object.getString(KEY_MAPPING));
		if (object.has(KEY_ROUTING))
			type.setRoutingField(object.getString(KEY_ROUTING));

		return type;
	}

	@Override
	public String toString() {
		JSONObject object = new JSONObject();
		object.put(KEY_NAME, name);
		object.put(KEY_INPUTPATH, inputPath);
		object.put(KEY_SPLITTER, Utils.splitterToStr(fieldSplitter));
		object.put(KEY_FIELDNAMES, Utils.join(fieldNames, ','));
		if (null != mapping)
			object.put(KEY_MAPPING, mapping);
		if (null != routingField)
			object.put(KEY_ROUTING, routingField);

		return object.toString();
	}

	private Integer routingFieldIndex;
	public int getRoutingFieldIndex() {
		if(null == routingField)
			return -1;
		if(routingFieldIndex != null)
			return routingFieldIndex;
		routingFieldIndex = Utils.indexOf(fieldNames, routingField);
		return routingFieldIndex;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	public char getFieldSplitter() {
		return fieldSplitter;
	}

	public void setFieldSplitter(char fieldSplitter) {
		this.fieldSplitter = fieldSplitter;
	}

	public String getMapping() {
		return mapping;
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
	}

	public String getRoutingField() {
		return routingField;
	}

	public void setRoutingField(String routingField) {
		this.routingField = routingField;
	}

}
