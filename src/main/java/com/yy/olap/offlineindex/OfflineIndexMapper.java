package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.exceptions.ConfigException;
import com.yy.olap.offlineindex.exceptions.MapperException;
import com.yy.olap.offlineindex.utils.ConfigUtil;
import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.*;

/**
 * @author colin.ke keqinwu@163.com
 */
public class OfflineIndexMapper extends Mapper<Object, Text, Text, Text> {

	private Log logger = LogFactory.getLog("MAPPER");

	public static final char KEY_SEPARATOR = '#';

	private String indexName;
	private int shardNum;
	private String[] fieldNames;
	private char fieldSplitter;
	private String routing;
	private String[] partitionKeys;
	private int routingFieldIndex;
	private Integer[] pKeysIndices;
	private String typeName;
	private Map<String, Set<Integer>> indicesShards = new HashMap<>();
	private boolean useIndexContext = false;

	private IndexContext indexContext;
	private Map<String, TypeContext> typeContextMap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		useIndexContext = conf.getBoolean(ConfigUtil.CONF_IS_MULTITYPE, false);
		if (useIndexContext) {
			String indexContextStr = conf.get(ConfigUtil.CONF_INDEX_CONTEXT);
			if (null == indexContextStr)
				throw new ConfigException("cannot found index context in config with key: " + ConfigUtil.CONF_INDEX_CONTEXT);
			indexContext = IndexContext.fromString(indexContextStr);
			typeContextMap = new HashMap<>();
			for (TypeContext type : indexContext.getTypes())
				typeContextMap.put(type.getName(), type);

			indexName = indexContext.getName();
			shardNum = indexContext.getNumberOfShards();
			partitionKeys = indexContext.getPartitionFields();

		} else {
			indexName = conf.get(JobHelper.CONF_KEY_INDEX);
			shardNum = conf.getInt(JobHelper.CONF_KEY_SHARD_NUM, 5);
			fieldNames = conf.getStrings(JobHelper.CONF_KEY_FIELDS);
			fieldSplitter = (char) conf.getInt(JobHelper.CONF_KEY_FIELD_SPLITTER, '\01');
			partitionKeys = conf.getStrings(JobHelper.CONF_KEY_PARTITION);
			routing = conf.get(JobHelper.CONF_KEY_ROUTING);
			typeName = conf.get(JobHelper.CONF_KEY_TYPE);

			if (hasRouting()) {
				routingFieldIndex = Utils.indexOf(fieldNames, routing);
				if (routingFieldIndex == -1)
					throw new MapperException("cannot find the routing: " + routing + " in the given fieldNames:" + Utils.join(fieldNames, ','));
			}

			if (needPartition()) {
				pKeysIndices = new Integer[partitionKeys.length];
				for (int i = 0; i < pKeysIndices.length; ++i) {
					int t = Utils.indexOf(fieldNames, partitionKeys[i]);
					if (-1 == t)
						throw new MapperException("cannot find the partition key: " + partitionKeys[i] + " in the given fieldNames:" + Utils.join(fieldNames, ','));
					pKeysIndices[i] = t;
				}
			}
		}
	}

	private boolean hasRouting() {
		return null != routing;
	}

	private boolean needPartition() {
		return null != partitionKeys;
	}

	private String getCompoundPartitionKey(String[] line) {
		if (!needPartition())
			return "";
		String[] keys = new String[pKeysIndices.length];
		for (int i = 0; i < pKeysIndices.length; ++i)
			keys[i] = line[pKeysIndices[i]];

		return Utils.join(keys, JobHelper.PARTITION_KEYS_SEPARATOR);
	}

	@Override
	protected void map(Object key, Text value, Context context) {

		try {
			if (useIndexContext) {
				typeName = key.toString();
				TypeContext type = typeContextMap.get(typeName);
				fieldNames = type.getFieldNames();
				fieldSplitter = type.getFieldSplitter();

				routing = type.getRoutingField();
				if (hasRouting())
					routingFieldIndex = type.getRoutingFieldIndex();


				if (needPartition())
					pKeysIndices = indexContext.getPkeyIndexes(typeName);
			}

			String[] fields = Utils.split(value.toString(), fieldSplitter);
			if (fields.length != fieldNames.length)
				logger.error("列数不一致: lineFieldCount:" + fields.length + ",期望:" + fieldNames.length + " - " + value);

			int shardId;
			if (hasRouting()) {
				if (fields[routingFieldIndex].isEmpty() || fields[routingFieldIndex].equals("\\N"))
					return;
				shardId = Math.abs(fields[routingFieldIndex].hashCode() % shardNum);
			} else {
				shardId = Math.abs(Strings.randomBase64UUID().hashCode() % shardNum);
			}

			String index;
			if (needPartition()) {
				index = indexName + JobHelper.PARTITION_INDEX_KEY_SEPARATOR + getCompoundPartitionKey(fields);
			} else {
				index = indexName;
			}

			String reduceKey = index + KEY_SEPARATOR + shardId;
			collectIndexShardId(index, shardId);
			context.write(new Text(reduceKey), new Text(typeName + Utils.toJson(fields, fieldNames, typeName)));
		} catch (Exception e) {
			logger.error("error in mapper", e);
			logger.info("key:" + key + ", value:" + value);
			logger.info("fieldNames: " + Arrays.toString(context.getConfiguration().getStrings(JobHelper.CONF_KEY_FIELDS)));
			throw new MapperException(e);
		}
	}

	private void collectIndexShardId(String index, int shardId) {
		Set<Integer> shardIdSet = indicesShards.get(index);
		if (null == shardIdSet) {
			shardIdSet = new HashSet<>();
			indicesShards.put(index, shardIdSet);
		}
		shardIdSet.add(shardId);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<String, Set<Integer>> entry : indicesShards.entrySet()) {
			// entry.key   -> indexName
			// entry.value -> shardId set
			if (entry.getValue().size() != shardNum) {
				for (int i = 0; i < shardNum; ++i) {
					if (entry.getValue().contains(i))
						continue;
					context.write(new Text(entry.getKey() + KEY_SEPARATOR + i), new Text(""));
				}
			}
		}
		super.cleanup(context);
	}
}
