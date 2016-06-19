package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.exceptions.ConfigException;
import com.yy.olap.offlineindex.exceptions.ReducerException;
import com.yy.olap.offlineindex.output.HdfsSyncingLocalFileOutputCommitter;
import com.yy.olap.offlineindex.utils.ConfigUtil;
import com.yy.olap.offlineindex.utils.EsUtils;
import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author colin.ke keqinwu@163.com
 */
public class OfflineIndexReducer extends Reducer<Text, Text, IntWritable, Text> {

	private Log logger = LogFactory.getLog("REDUCER");
	private static final String TMP_INDEX_NAME = "tmp_index";

	private Node esNode;
	private DocumentMapper docMapper;
	private Map<String, DocumentMapper> docMapperMap;
	private String type;
	private Map<String, IndexWriter> shardIndexWriters;

	private boolean useIndexContext = false;
	private IndexContext indexContext;
	private Map<String, TypeContext> typeContextMap;
	private IndexService indexService;

	@Override
	public void setup(Context context) {
		try {
			Configuration config = context.getConfiguration();
			esNode = createAndStartNode();

			useIndexContext = config.getBoolean(ConfigUtil.CONF_IS_MULTITYPE, false);
			if(useIndexContext) {
				String indexContextStr = config.get(ConfigUtil.CONF_INDEX_CONTEXT);
				if (null == indexContextStr)
					throw new ConfigException("cannot found index context in config with key: " + ConfigUtil.CONF_INDEX_CONTEXT);
				indexContext = IndexContext.fromString(indexContextStr);
				typeContextMap = new HashMap<>();
				for (TypeContext type : indexContext.getTypes())
					typeContextMap.put(type.getName(), type);

				// create index with default mapping
				indexService = createIndex(TMP_INDEX_NAME, "_default_", EsUtils.getDefaultMapping("_default_"));

			} else {
				type = config.get(JobHelper.CONF_KEY_TYPE);
				String typeMapping = config.get(JobHelper.CONF_KEY_MAPPING, EsUtils.getDefaultMapping(type));
				docMapper = createIndex(TMP_INDEX_NAME, type, typeMapping).mapperService().documentMapper(type);
			}

			docMapperMap = new HashMap<>();
			shardIndexWriters = new HashMap<>();
		} catch (Exception e) {
			logger.error("error in reducer.setup", e);
			throw new ReducerException(e);
		}
	}


	private IndexWriter getIndexWriter(String indexShard, Context context) throws IOException {
		IndexWriter writer = shardIndexWriters.get(indexShard);
		if (null == writer) {
			File localShardDir = ((HdfsSyncingLocalFileOutputCommitter) context.getOutputCommitter()).getLocalScratchPath(indexShard, context);
			logger.info("creating indexWriter, local shard dir: " + localShardDir.getPath());
			writer = new IndexWriter(FSDirectory.open(localShardDir), EsUtils.createIndexWriterConfig());
			shardIndexWriters.put(indexShard, writer);
		}
		return writer;
	}

	private void noteEmtpySet(String key, Context context) {
		String[] pair = Utils.split(key, OfflineIndexMapper.KEY_SEPARATOR);
		if (pair.length != 2) {
			logger.warn("unexpected key format: " + key);
			return;
		}
		((HdfsSyncingLocalFileOutputCommitter) context.getOutputCommitter()).noteEmptyShard(pair[0], pair[1]);
	}

	private void puttingTypeMapping(String type, String mapping) {
		PutMappingRequest request = Requests.putMappingRequest(TMP_INDEX_NAME).type(type).source(mapping);
		esNode.client().admin().indices().putMapping(request).actionGet();
	}

	/**
	 * @param key {indexName}-{shardnum}
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) {
		try {
			logger.info("key: " + key);
			IndexWriter indexWriter = getIndexWriter(key.toString(), context);
			Map<String, Integer> typeCounter = new HashMap<>();
			int t;
			String doc;
			TypeContext typeContext;
			for (Text docText : values) {
				if (docText.toString().trim().isEmpty())
					continue;
				t = docText.toString().indexOf('{');
				type = docText.toString().substring(0, t);
				doc = docText.toString().substring(t);

				if(useIndexContext) {
					docMapper = docMapperMap.get(type);
					if(null == docMapper) {
						typeContext = typeContextMap.get(type);
						if(null != typeContext.getMapping()) {
							puttingTypeMapping(type, typeContext.getMapping());
						} else {
							puttingTypeMapping(type, EsUtils.getDefaultMapping(type));
						}
						docMapper = indexService.mapperService().documentMapper(type);
						docMapperMap.put(type, docMapper);
					}
				}


				if(!typeCounter.containsKey(type))
					typeCounter.put(type, 0);
				if (typeCounter.get(type) < 50) {
					// 插多几条，确保获得每一列的mapping
					String tmpDoc = doc.replaceAll(",\"_id\":\".*\"", "");
					indexEsNode(tmpDoc, TMP_INDEX_NAME, type);
					typeCounter.put(type, typeCounter.get(type) + 1);
				}
				try {
					ParsedDocument parsedDocument = docMapper.parse(new BytesArray(doc.getBytes()));

					for (ParseContext.Document document : parsedDocument.docs()) {
						indexWriter.addDocument(document);
					}
				} catch (Exception e) {
					logger.error("error doc: " + docText);
					throw new ReducerException("error doc:" + docText, e);
				}
			}
			if (typeCounter.isEmpty()) {
				noteEmtpySet(key.toString(), context);
			}
			String mapping;
			String index = Utils.split(key.toString(), OfflineIndexMapper.KEY_SEPARATOR)[0];
			for(String theType : typeCounter.keySet()) {
				mapping = esNode.client().admin().indices().prepareGetMappings(TMP_INDEX_NAME).get()
						.getMappings().get(TMP_INDEX_NAME).get(theType).source().toString();
				logger.info("mapping: " + mapping);
				HdfsSyncingLocalFileOutputCommitter.setIndexMapping(index, theType, mapping);
			}
		} catch (Exception e) {
			logger.error("error in reduce", e);
			throw new ReducerException(e);
		}
	}

	private void indexEsNode(String doc, String index, String type) {
		esNode.client().prepareIndex(index, type).setSource(doc).get();
	}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (IndexWriter writer : shardIndexWriters.values()) {
			writer.commit();
			writer.close();
		}
		esNode.close();
	}

	private Node createAndStartNode() {
		Node node = EsUtils.createMemNode();
		node.start();
		return node;
	}

	IndexService createIndex(String index, String type, String typeMapping) {
		if(null == typeMapping)
			typeMapping = EsUtils.getDefaultMapping(type);
		return EsUtils.createIndex(esNode, index, type, typeMapping);
	}
}
