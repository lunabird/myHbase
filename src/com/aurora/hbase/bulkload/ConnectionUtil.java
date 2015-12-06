package com.aurora.hbase.bulkload;

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class ConnectionUtil {

	public static Configuration hBaseConfiguration;
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
		
		hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.set("hbase.master", "192.168.0.120:60000");
		hBaseConfiguration.set("hbase.zookeeper.quorum", "192.168.0.120,192.168.0.121,192.168.0.122,192.168.0.123,192.168.0.124," +
				"192.168.0.125,192.168.0.126,192.168.0.127,192.168.0.128,192.168.0.129,192.168.0.130");//配置Zookeeper集群的地址列表
		hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
	}

	
	
	
	public static HTable getTable() throws IOException {
		// TODO Auto-generated method stub
		HTable wordCountTable = new HTable(hBaseConfiguration, "word_count");
		return wordCountTable;
	}

	public static Configuration getConfiguration() {
		// TODO Auto-generated method stub
		return hBaseConfiguration;
	}

}
