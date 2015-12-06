package com.sample.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * 此包包含了一个mapreduce程序的示例。用来学习MapReduce编程。
 * 与极光数据无关。
 * @author huangpeng
 * @date 2015/3/11
 *
 */
public class PrintUserCount {

	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.master", "192.168.0.120 :60000");
		configuration.set("hbase.zookeeper.quorum", "192.168.0.120");//配置Zookeeper集群的地址列表
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
    public static void main(String[] args) throws Exception {

//        HBaseConfiguration conf = new HBaseConfiguration();
        HTable htable = new HTable(configuration, "summary_user");

        Scan scan = new Scan();
        ResultScanner scanner = htable.getScanner(scan);
        Result r;
        while (((r = scanner.next()) != null)) {
            ImmutableBytesWritable b = r.getBytes();
            byte[] key = r.getRow();
            int userId = Bytes.toInt(key);
            byte[] totalValue = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("total"));
            int count = Bytes.toInt(totalValue);

            System.out.println("key: " + userId+ ",  count: " + count);
        }
        scanner.close();
        htable.close();
    }
}