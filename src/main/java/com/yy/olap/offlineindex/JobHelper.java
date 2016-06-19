package com.yy.olap.offlineindex;

import com.yy.olap.offlineindex.utils.Utils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * @author colin.ke keqinwu@163.com
 */
public class JobHelper {

	private static Log logger = LogFactory.getLog(JobHelper.class);

	public static final char FIELD_NAME_SEPARATOR = ',';
	public static final char PARTITION_KEYS_SEPARATOR = '-';
	public static final char PARTITION_INDEX_KEY_SEPARATOR = '@';
	public static final String HDFS_LIB_DIR_PATH = "/tmp/es_offline_index/";

	public static final String CONF_KEY_INDEX = "es.index";
	public static final String CONF_KEY_TYPE = "es.type";
	public static final String CONF_KEY_SHARD_NUM = "es.shard.num";
	public static final String CONF_KEY_MAPPING = "es.typeMapping";
	public static final String CONF_KEY_PARTITION = "es.partition.keys";
	public static final String CONF_KEY_ROUTING = "es.routing";
	public static final String CONF_KEY_FIELDS = "data.field.names";
	public static final String CONF_KEY_FIELD_SPLITTER = "data.field.splitter";

	//optional params
	public static final String PARAM_KEY_PARTITION = "partition";
	public static final String PARAM_KEY_ROUTING = "routing";
	public static final String PARAM_KEY_MAPPING = "mapping";
	private static final String OPTIONAL_PARAM_PATTERN = "((" + PARAM_KEY_PARTITION + ")|(" + PARAM_KEY_ROUTING + ")|(" + PARAM_KEY_MAPPING + "))=.+";

//	private static final String[] OPTIONAL_PARAM_KEYS = new String[]{PARAM_KEY_MAPPING, PARAM_KEY_ROUTING, PARAM_KEY_PARTITION};

	private static final String OUT_DIR = "/tmp/offline_index";

	//required params
	private String indexName;
	private String typeName;
	private int numberOfShards;
	private URL dataFilePath;
	private String[] fieldNames;
	private char fieldSplitter;
	private String esIndicesPath;
	private String esAddress;

	private Map<String, String> optionalParams = new HashMap<>();


	static {
		Utils.registeredURLStreamHandler();
	}

	private JobHelper() {
	}

	public static JobFuture execute(String hadoopHome, IndexContext indexContext) throws IOException {
		String jarPath = JobMain.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (!jarPath.endsWith(".jar")) {
			logger.error("cannot find jarPath");
			return null;
		}
		hadoopHome = Utils.ensureEndWithFileSeparator(hadoopHome);
		logger.info("hadoopHome: " + hadoopHome);
		logger.info("map-reduce job jar path: " + jarPath);

		String properties = indexContext.toProperties();
		CommandLine commandLine = CommandLine.parse(hadoopHome + "bin/hadoop jar " + jarPath + " " + properties);

		DefaultExecutor executor = new DefaultExecutor();

		File dir = new File(OUT_DIR);
		if(!dir.exists())
			dir.mkdir();
		String outFilePath = Utils.ensureEndWithFileSeparator(OUT_DIR) + indexContext.getName() +  "_" + System.currentTimeMillis();
		logger.info("std out and err has been redirect to :" + outFilePath);
		File outFile = new File(outFilePath);
		outFile.createNewFile();
		executor.setStreamHandler(new PumpStreamHandler(new FileOutputStream(outFile)));
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		executor.execute(commandLine, resultHandler);

		JobFuture jobFuture = new JobFuture(resultHandler, outFilePath);
		jobFuture.setIndex(indexContext.getName());
		for(TypeContext type : indexContext.getTypes()) {
			jobFuture.addType(type.getName());
		}
		return jobFuture;
	}

	@Deprecated
	public JobFuture execute(String hadoopHome, String jarPath) throws Exception {
		logger.info("hadoopHome: " + hadoopHome);
		logger.info("map-reduce job jar path: " + jarPath);

		if (!hadoopHome.endsWith(File.separator))
			hadoopHome = hadoopHome + File.separator;

		CommandLine commandLine = CommandLine.parse(hadoopHome + "bin/hadoop jar " + jarPath);

		//main class信息已经在打包的时候写进manifest文件，所以参数不需要传
		commandLine.addArgument(indexName);
		commandLine.addArgument(typeName);
		commandLine.addArgument(String.valueOf(numberOfShards));
		commandLine.addArgument(dataFilePath.toExternalForm());
		commandLine.addArgument(Utils.join(fieldNames, FIELD_NAME_SEPARATOR));
		commandLine.addArgument(fieldSplitter == '\01' ? "u0001" : String.valueOf(fieldSplitter));
		commandLine.addArgument(esIndicesPath);
		commandLine.addArgument(esAddress);


		for (Map.Entry entry : optionalParams.entrySet()) {
			commandLine.addArgument(entry.getKey() + "=" + entry.getValue());
		}
		DefaultExecutor executor = new DefaultExecutor();
		String outFilePath = "/tmp/offline_index/outAndErr_" + System.currentTimeMillis();
		logger.info("std out and err has been redirect to :" + outFilePath);
		File outFile = new File(outFilePath);
		executor.setStreamHandler(new PumpStreamHandler(new FileOutputStream(outFile)));
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		executor.execute(commandLine, resultHandler);

		JobFuture jobFuture = new JobFuture(resultHandler, outFilePath);
		jobFuture.setIndex(indexName);
		jobFuture.addType(typeName);

		return jobFuture;
	}

	@Deprecated
	public JobFuture execute(String hadoopHome) throws Exception {
		String jarPath = JobMain.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (!jarPath.endsWith(".jar")) {
			logger.error("cannot find jarPath");
			return null;
		}
		return execute(hadoopHome, jarPath);
	}

	public JobHelper(String indexName, String typeName, int numberOfShards, URL dataFilePath, String[] fieldNames, char fieldSplitter, String esIndicesPath, String esAddress) {
		this();
		this.indexName = indexName;
		this.typeName = typeName;
		this.numberOfShards = numberOfShards;
		this.dataFilePath = dataFilePath;
		this.fieldNames = fieldNames;
		this.fieldSplitter = fieldSplitter;
		this.esIndicesPath = esIndicesPath;
		this.esAddress = esAddress;
	}

	public static JobHelper parseFromArgs(String[] args) {

		int required = 8;
		if (args.length < required)
			throw new IllegalArgumentException("required arguments: indexName, typeName, numOfShards, dataFilePath, fields, fieldSplitter, esIndicesPath, esAddress");
		for (int i = 0; i < args.length; ++i) {
			args[i] = unwrap(args[i]);
		}
		try {
			JobHelper job = new JobHelper();
			job.indexName = args[0];
			job.typeName = args[1];
			job.numberOfShards = Integer.parseInt(args[2]);
			job.dataFilePath = new URL(args[3]);
			job.fieldNames = Utils.split(args[4], FIELD_NAME_SEPARATOR);
			job.fieldSplitter = args[5].equals("u0001") ? '\01' : args[5].charAt(0);
			job.esIndicesPath = args[6];
			job.esAddress = args[7];
			for (int i = required; i < args.length; ++i)
				parseOptionalParam(args[i], job);
			return job;

		} catch (MalformedURLException | NumberFormatException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static String unwrap(String arg) {
		if (arg.charAt(0) == '\'' && arg.charAt(arg.length() - 1) == '\'' ||
				arg.charAt(0) == '"' && arg.charAt(arg.length() - 1) == '"')
			return arg.substring(1, arg.length() - 1);
		return arg;
	}

	private static void parseOptionalParam(String paramStr, JobHelper job) {
		if (!paramStr.matches(OPTIONAL_PARAM_PATTERN))
			throw new IllegalArgumentException("the optional params (" + paramStr + ")'s format should match " + OPTIONAL_PARAM_PATTERN);
		String[] pair = Utils.split(paramStr, '=');
		job.optionalParams.put(pair[0], pair[1]);
	}

	public String getEsIndicesPath() {
		return esIndicesPath;
	}

	public String getEsAddress() {
		return esAddress;
	}

	public String[] getPartitionFields() {
		if (optionalParams.containsKey(PARAM_KEY_PARTITION))
			return Utils.split(optionalParams.get(PARAM_KEY_PARTITION), PARTITION_KEYS_SEPARATOR);
		return null;
	}

	public String getRoutingField() {
		return optionalParams.get(PARAM_KEY_ROUTING);
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}


	public char getFieldSplitter() {
		return fieldSplitter;
	}

	public String getIndexName() {
		return indexName;
	}


	public URL getDataFilePath() {
		return dataFilePath;
	}


	public String[] getFieldNames() {
		return fieldNames;
	}


	public String getTypeName() {
		return typeName;
	}


	public String getTypeMapping() {
		return optionalParams.get(PARAM_KEY_MAPPING);
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public void setNumberOfShards(int numberOfShards) {
		this.numberOfShards = numberOfShards;
	}

	public void setDataFilePath(URL dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	public void setFieldSplitter(char fieldSplitter) {
		this.fieldSplitter = fieldSplitter;
	}

	public void setEsIndicesPath(String esIndicesPath) {
		this.esIndicesPath = esIndicesPath;
	}

	public void setEsAddress(String esAddress) {
		this.esAddress = esAddress;
	}

	public void setPartitionFields(String[] partitionFields) {
		optionalParams.put(PARAM_KEY_PARTITION, Utils.join(partitionFields, PARTITION_KEYS_SEPARATOR));
	}

	public void setRoutingField(String routingField) {
		optionalParams.put(PARAM_KEY_ROUTING, routingField);
	}

	public void setTypeMapping(String typeMapping) {
		optionalParams.put(PARAM_KEY_MAPPING, typeMapping);
	}
}
