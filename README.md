# Elasticsearch Offline Index

Sometimes you just need to import huge amount of data into Elasticsearch, and you want do that with offline instead of using es client which will consume es cluster resources and influences the search performance.  
Now you can use this tool to create es index by running a map-reduce task and then copy the index file to es cluster, which can be higher throughput.

You can found sample code in *com.yy.olap.offlineindex.sample.Sample.java*
