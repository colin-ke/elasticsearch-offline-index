package com.yy.olap.offlineindex.utils;

import com.google.common.collect.Maps;
import com.yy.elasticsearch.dateagg.util.DateAggSettingFormatter;
import com.yy.olap.offlineindex.exceptions.OfflineIndexException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * @author colin.ke keqinwu@163.com
 */
public class EsUtils {

	public static Node createMemNode() {
		return NodeBuilder.nodeBuilder().local(true).data(true).settings(ImmutableSettings.builder()
				.put(ClusterName.SETTING, "offline-index")
				.put("node.name", "offline-index")
				.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
				.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
				.put("script.disable_dynamic", false)
				.put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
				.put("http.enabled", false)
				.put("index.store.type", "ram")
				.put("config.ignore_system_properties", true) // make sure we get what we set :)
				.put("discovery.zen.ping.multicast.enabled", false)
				.put("gateway.type", "none")).build();
	}

	public static IndexService createIndex(Node esNode, String index, String type, String typeMapping) {
		CreateIndexRequestBuilder requestBuilder = esNode.client().admin().indices().prepareCreate(index);
		requestBuilder.addMapping(type, typeMapping);
		requestBuilder.get();
		IndicesService indicesService = ((InternalNode) esNode).injector().getInstance(IndicesService.class);
		return indicesService.indexServiceSafe(index);
	}

	public static String getDefaultMapping(String type) {
		return "{\"" + type + "\":{\"dynamic_templates\":[{\"str\":{\"mapping\":{\"type\":\"string\",\"index\":\"not_analyzed\"}," +
				"\"match\":\"*\",\"match_mapping_type\":\"string\"}}],\"date_detection\":false}}";
	}

	public static IndexWriterConfig createIndexWriterConfig() {
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LATEST, new StandardAnalyzer());
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		indexWriterConfig.setMergePolicy(new TieredMergePolicy());
		indexWriterConfig.setRAMBufferSizeMB(2048);
		indexWriterConfig.setUseCompoundFile(false);
		return indexWriterConfig;
	}

	public static String genIndexMetaFile(String index, int shards, int replicas, int versionNum, Map<String, String> typeMappings, boolean isOpen) throws IOException {
		ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
				.put(IndexMetaData.SETTING_UUID, Strings.randomBase64UUID())
				.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shards)
				.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, replicas)
				.put(IndexMetaData.SETTING_VERSION_CREATED, versionNum)
				.put(IndexMetaData.SETTING_CREATION_DATE, System.currentTimeMillis());
		if(index.indexOf('@') != -1) {
			String partitionKey = index.substring(index.indexOf('@') + 1, index.length());
			settings.put(DateAggSettingFormatter.buildSetting(partitionKey));
		}
		IndexMetaData.Builder metaBuilder = new IndexMetaData.Builder(index).settings(settings).state(isOpen ? IndexMetaData.State.OPEN : IndexMetaData.State.CLOSE);
		if (null != typeMappings) {
			for (Map.Entry<String, String> mapping : typeMappings.entrySet()) {
				metaBuilder.putMapping(mapping.getKey(), mapping.getValue());
			}
		}
		IndexMetaData indexMetaData = metaBuilder.build();

		String tmpStateFile = Utils.getTmpDirPath() + MetaData.INDEX_STATE_FILE_PREFIX + versionNum + MetaData.STATE_FILE_EXTENSION;
		MetaData.write(indexMetaData, tmpStateFile, versionNum);
		return tmpStateFile;
	}

	/**
	 * default opened index
	 *
	 * @return
	 * @throws IOException
	 */
	public static String genIndexMetaFile(String index, int shards, int replicas, int versionNum, Map<String, String> typeMappings) throws IOException {
		return genIndexMetaFile(index, shards, replicas, versionNum, typeMappings, true);
	}

	/**
	 * get es client from address(host:port)
	 *
	 * @param address the format should be '{host}:{port}'
	 * @return
	 */
	public static TransportClient getClient(String address) {
		String pattern = "(\\d{1,3}\\.){3}\\d+:\\d{1,5}";
		if (!address.matches(pattern))
			throw new OfflineIndexException("wrong es address format: " + address + ", which should match " + pattern);

		Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", true).build();
		TransportClient client = new TransportClient(settings);
		String[] ipPort = address.split(":");
		client.addTransportAddress(new InetSocketTransportAddress(ipPort[0], Integer.parseInt(ipPort[1])));
		return client;
	}

	public static String randomBase64UUID() {
		return Strings.randomBase64UUID();
	}


	public static class MetaData {
		public static final String INDEX_STATE_FILE_PREFIX = "state-";
		public static final String STATE_FILE_EXTENSION = ".st";
		private static final String STATE_FILE_CODEC = "state";
		private static final int STATE_FILE_VERSION = 0;
		private static final int BUFFER_SIZE = 4096;

		private static XContentType format = XContentType.fromRestContentType("smile");
		private static ToXContent.Params formatParams;

		static {
			Map<String, String> params = Maps.newHashMap();
			params.put("binary", "true");
			formatParams = new ToXContent.MapParams(params);
		}


		public static void write(IndexMetaData state, String tmpStatePath, long version) throws IOException {
			File st = new File(tmpStatePath);
			if (!st.exists()) {
				int retry = 10;
				for (int i = 0; i < retry && !st.createNewFile(); ++i) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException ignored) {
					}
				}
				if (!st.exists()) {
					throw new OfflineIndexException("failed to create tmp state file:" + tmpStatePath);
				}
			}
			try (OutputStreamIndexOutput out = new OutputStreamIndexOutput(Files.newOutputStream(Paths.get(tmpStatePath)), BUFFER_SIZE)) {
				CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
				out.writeInt(format.index());
				out.writeLong(version);
				try (XContentBuilder builder = newXContentBuilder(format, new org.elasticsearch.common.lucene.store.OutputStreamIndexOutput(out) {
					@Override
					public void close() throws IOException {
						// this is important since some of the XContentBuilders write bytes on close.
						// in order to write the footer we need to prevent closing the actual index input.
					}
				})) {

					builder.startObject();
					{
						IndexMetaData.Builder.toXContent(state, builder, formatParams);
					}
					builder.endObject();
				}
				CodecUtil.writeFooter(out);
			}
		}

		static XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
			return XContentFactory.contentBuilder(type, stream);
		}
	}

}
