/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-20
 */
package com.sample.hbase;

/**
 * @author hadoop
 *
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.aroura.hbase.util.HBaseJavaAPI;
import com.aurora.hbase.hdfs.CopyToHDFS;

public class HBaseImportEx {
//	static Configuration hbaseConfig = null;
	
//	static {
//		// conf = HBaseConfiguration.create();
//		Configuration HBASE_CONFIG = new Configuration();
//		HBASE_CONFIG.set("hbase.master", "192.168.230.133:60000");
//		HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.230.133");
//		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
//		hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);
//
//		pool = new HTablePool(hbaseConfig, 1000);
//	}
	public static HTablePool pool = null;
	public static String tableName = "HAuroraImgTable";
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.master", "192.168.0.120:60000");
		conf.set("hbase.zookeeper.quorum", "192.168.0.120,192.168.0.121,192.168.0.122,192.168.0.123,192.168.0.124," +
				"192.168.0.125,192.168.0.126,192.168.0.127,192.168.0.128,192.168.0.129,192.168.0.130");//配置Zookeeper集群的地址列表
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		pool = new HTablePool(conf, 1000); 
	}
	
	
	public static Configuration getHBaseConf(){
		return conf;
	}
	
	
	/*
	 * Insert Test single thread
	 */
	public static void SingleThreadInsert() throws IOException {
		System.out.println("---------开始SingleThreadInsert测试----------");
		long start = System.currentTimeMillis();
		// HTableInterface table = null;
		HTable table = null;
		table = (HTable) pool.getTable(tableName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(24 * 1024 * 1024);
		// 构造测试数据
		List<Put> list = new ArrayList<Put>();
		int count = 10000;
		byte[] buffer = new byte[350];
		Random rand = new Random();
		for (int i = 0; i < count; i++) {
			Put put = new Put(String.format("row %d", i).getBytes());
			rand.nextBytes(buffer);
			put.add("f1".getBytes(), null, buffer);
			// wal=false
			put.setWriteToWAL(false);
			list.add(put);
			if (i % 10000 == 0) {
				table.put(list);
				list.clear();
				table.flushCommits();
			}
		}
		long stop = System.currentTimeMillis();
		// System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

		System.out.println("插入数据：" + count + "共耗时：" + (stop - start) * 1.0
				/ 1000 + "s");

		System.out.println("---------结束SingleThreadInsert测试----------");
	}

	/*
	 * 多线程环境下线程插入函数
	 */
	public static void InsertProcess() throws IOException {
		long start = System.currentTimeMillis();
		// HTableInterface table = null;
		HTableInterface table = null;
		table = pool.getTable(tableName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(5 * 1024 * 1024);
		// 构造测试数据
//		List<Put> list = new ArrayList<Put>();
//		int count = 10000;
//		byte[] buffer = new byte[256];
//		Random rand = new Random();
//		for (int i = 0; i < count; i++) {
//			Put put = new Put(String.format("row %d", i).getBytes());
//			rand.nextBytes(buffer);
//			put.add("f1".getBytes(), null, buffer);
//			// wal=false
//			put.setWriteToWAL(false);
//			list.add(put);
//			if (i % 10000 == 0) {
//				table.put(list);
//				list.clear();
//				table.flushCommits();
//			}
//		}
		
		ArrayList<String> list = CopyToHDFS.readLogTxt();	
		
		List<Put> lp = new ArrayList<Put>();  
		for(int i=0;i<list.size();i++){
			String sourcePath = list.get(i);
			String[] tmp = sourcePath.split("\\\\");
			String targetPath = "E:/AuroraRawData-2004/200411/N20041109G_F/"+tmp[tmp.length-1];
			String fileName = targetPath;
			//System.out.println("fileName:"+fileName);
			File file = new File(fileName);
			byte[] content = FileUtils.readFileToByteArray(file);//将图片内容转换成字节流
			String myRowKey = tmp[tmp.length-1].split("\\.")[0];
			char[] rk = myRowKey.toCharArray();
			//生成表HAurora表的行键
			String rowkey = rk[0]+"01"+rk[1]+rk[2]+rk[3]+rk[4]+rk[5]+rk[6]+rk[8]+rk[9]+rk[10]+rk[11]+rk[12]+rk[7];
			//向表中添加行
		
			Put put = new Put(Bytes.toBytes(rowkey));
			put.setWriteToWAL(false);
//			put.setWriteToWAL(true);
			put.add(Bytes.toBytes("data"), null, content);
			lp.add(put);
			if(i%100 == 0){
				table.put(lp);
				lp.clear();
			}
			table.put(put);
			//System.out.println(rowkey+"  loaded successfully!");
		}
		table.put(lp);
		table.close();//提交flush
		long stop = System.currentTimeMillis();
		// System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

		System.out.println("线程:" + Thread.currentThread().getId() + "插入数据："
				+ list.size() + "共耗时：" + (stop - start) + "ms");
	}

	/*
	 * Mutil thread insert test
	 */
	public static void MultThreadInsert() throws InterruptedException {
		System.out.println("---------开始MultThreadInsert测试----------");
		long start = System.currentTimeMillis();
		int threadNumber = 5;
		Thread[] threads = new Thread[threadNumber];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new ImportThread();
			threads[i].start();
		}
		for (int j = 0; j < threads.length; j++) {
			(threads[j]).join();
		}
		long stop = System.currentTimeMillis();

		System.out.println("MultThreadInsert：" + threadNumber  + "共耗时："
				+ (stop - start) * 1.0 / 1000 + "s");
		System.out.println("---------结束MultThreadInsert测试----------");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// SingleThreadInsert();
		HBaseJavaAPI.deleteTable("HAuroraImgTable");
		HBaseJavaAPI.createAuroraImgTable();
		
		MultThreadInsert();

	}

	public static class ImportThread extends Thread {
		public void HandleThread() {
			// this.TableName = "T_TEST_1";

		}

		public void run() {
			try {
				InsertProcess();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				System.gc();
			}
		}
	}

}