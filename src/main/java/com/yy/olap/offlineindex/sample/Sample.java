package com.yy.olap.offlineindex.sample;

import com.yy.olap.offlineindex.IndexContext;
import com.yy.olap.offlineindex.JobHelper;
import com.yy.olap.offlineindex.TypeContext;

import java.io.IOException;

/**
 * @auther colin.ke keqinwu@163.com
 */
public class Sample {
	public static void main(String[] args) throws IOException {
		IndexContext indexContext = new IndexContext();
		String indexName = "offline_index";
		int shardNum = 10;

		indexContext.setName(indexName);
		indexContext.setNumberOfShards(shardNum);
		indexContext.setEsAddress("127.0.0.1:9300");
		// the es indices data path
		indexContext.setEsIndicesPath("/data/elasticsearch/cluster1/nodes/0/indices");

		// OPTIONAL, if set, multiple indices will be create according to these partition keys.
		indexContext.setPartitionFields(new String[]{"year", "month", "day"});

		String[] fieldNames = new String[]{"year", "month", "day", "uid", "name", "age"};
		String typeName = "type1";
		String mapping = getDefaultMapping(typeName);

		TypeContext typeContext = new TypeContext();
		typeContext.setName(typeName);
		typeContext.setFieldNames(fieldNames);
		typeContext.setFieldSplitter(',');
		typeContext.setInputPath("/pathToInputDataOnHdfs");
		typeContext.setMapping(mapping);

		// OPTIONAL
		typeContext.setRoutingField("uid");

		indexContext.addTypeContext(typeContext);


		JobHelper.execute("/pathToHadoopHome", indexContext);
	}

	public static String getDefaultMapping(String type) {
		return "{\"" + type + "\":{\"dynamic_templates\":[{\"str\":{\"mapping\":{\"type\":\"string\",\"index\":\"not_analyzed\"}," +
				"\"match\":\"*\",\"match_mapping_type\":\"string\"}}],\"date_detection\":false}}";
	}
}
