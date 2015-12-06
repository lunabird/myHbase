package com.sample.hbase;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseQuery {

	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.master", "192.168.0.120 :60000");
		configuration.set("hbase.zookeeper.quorum", "192.168.0.120");//配置Zookeeper集群的地址列表
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	public static void main(String[] args){
		try {
			QueryAll("RawImgTable");
//			QueryByCondition1("RawImgTable");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} //查询遍历数据
	}
	
	/**
	 * 遍历查询
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void QueryAll(String tableName) throws IOException {
		// HTablePool pool = new HTablePool(configuration, 1000);
		// HTable table = (HTable) pool.getTable(tableName);
		HTable table = new HTable(configuration, tableName);
		try {
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + keyValue.getValue().length);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 条件查询 根据rowkey进行条件查询
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void QueryByCondition1(String tableName) throws IOException {

		// HTablePool pool = new HTablePool(configuration, 1000);
		// HTable table = (HTable) pool.getTable(tableName);
		HTable table = new HTable(configuration, tableName);
		try {
			Get scan = new Get("N041109G00001.img".getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("Get:"+r.toString());
			System.out.println("获得到rowkey:" + new String(r.getRow()));
//			for (KeyValue keyValue : r.raw()) {
			for (KeyValue keyValue : r.raw()) {
//				if(keyValue.matchingColumn("img_content".getBytes(), "raw".getBytes())){
				if(keyValue.matchingFamily("img_content".getBytes())){
					FileUtils.writeByteArrayToFile(new File("E:\\N041109G00001.img"), keyValue.getValue());
					System.out.println("列：" + new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())
							+ "====值:"+keyValue.getValue().length);//new String(keyValue.getValue())
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
