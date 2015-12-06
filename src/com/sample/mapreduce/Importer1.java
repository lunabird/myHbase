package com.sample.mapreduce;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @brief 此包包含了一个mapreduce程序的示例。用来学习MapReduce编程。与极光数据无关。
 * writes random access logs into hbase table
 * 
 *   userID_count => {
 *      details => {
 *          page
 *      }
 *   }
 * 
 * @author sujee ==at== sujee.net
 * @date 2015/3/11
 *
 */
public class Importer1 {
	
	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.master", "192.168.0.120 :60000");
		configuration.set("hbase.zookeeper.quorum", "192.168.0.120");//配置Zookeeper集群的地址列表
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}

    public static void main(String[] args) throws Exception {
        
        String [] pages = {"/", "/a.html", "/b.html", "/c.html"};
        
//        HBaseConfiguration hbaseConfig = new HBaseConfiguration();
        HTable htable = new HTable(configuration, "access_logs");
        htable.setAutoFlush(false);
        htable.setWriteBufferSize(1024 * 1024 * 12);
        
        int totalRecords = 100000;
        int maxID = totalRecords / 1000;
        Random rand = new Random();
        System.out.println("importing " + totalRecords + " records ....");
        for (int i=0; i < totalRecords; i++)
        {
            int userID = rand.nextInt(maxID) + 1;
            byte [] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
            String randomPage = pages[rand.nextInt(pages.length)];
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("details"), Bytes.toBytes("page"), Bytes.toBytes(randomPage));
            htable.put(put);
        }
        htable.flushCommits();
        htable.close();
        System.out.println("done");
    }
}
