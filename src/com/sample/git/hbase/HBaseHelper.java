package com.sample.git.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Diego Pino García <dpino@igalia.com>
 * 
 */
public class HBaseHelper {

	private static final Configuration conf = HBaseConfiguration.create();
	private static final HTablePool tablePool = new HTablePool(conf, Integer.MAX_VALUE);

	public static HBaseHelper create() {
		HBaseHelper result = new HBaseHelper();
		try {
			result.hbase = new HBaseAdmin(conf);
			return result;
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}

	private HBaseAdmin hbase;

	static {
		conf.set("hbase.master", "192.168.137.135 :60000");
		conf.set("hbase.zookeeper.quorum", "192.168.137.135");//配置Zookeeper集群的地址列表
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	/*public static void main(String[] args) throws IOException{
		HBaseHelper p = new HBaseHelper();
		p.doCreateTable2("AuroraDataTable");
	}*/
	
	
	private HBaseHelper() {

	}	

	public HTable createTable(String tableName, String... descriptors)
			throws IOException {
		if (tableExists(tableName)) {
			dropTable(tableName);
		}
//		return doCreateTable(tableName, descriptors);
		return doCreateTable(tableName);
	}

	/*private HTable doCreateTable(String tableName, String... descriptors)
			throws IOException {
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		for (String each : descriptors) {
			HColumnDescriptor cd = new HColumnDescriptor(each.getBytes());
			descriptor.addFamily(cd);
		}
		hbase.createTable(descriptor);
		debug(String.format("Database %s created", tableName));
		return new HTable(conf,tableName);
	}*/
	/**
	 * 创建表RawImgTable
	 * 
	 * @param tableName
	 * @throws IOException 
	 */
	private HTable doCreateTable(String tableName) throws IOException {
		System.out.println("start create table ......"+tableName);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			//第一个列簇,图片内容字节流，设置块大小为576kb
			tableDescriptor.addFamily(new HColumnDescriptor("img_content").setBlocksize(589824));
			//存储img图片的一些元信息
			tableDescriptor.addFamily(new HColumnDescriptor("img_meta"));
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......"+tableName);
		return new HTable(conf,tableName);
	}
	/**
	 * 创建表AuroraDataTable
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	private HTable doCreateTable2(String tableName) throws IOException {
		System.out.println("start create table ......"+tableName);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			//第一个列簇,图片内容字节流，设置块大小为256kb
			tableDescriptor.addFamily(new HColumnDescriptor("i_content").setBlocksize(262144));
			//存储图片的一些类型信息
			tableDescriptor.addFamily(new HColumnDescriptor("i_type"));
			tableDescriptor.addFamily(new HColumnDescriptor("i_mark"));
			tableDescriptor.addFamily(new HColumnDescriptor("i_meta"));
			tableDescriptor.addFamily(new HColumnDescriptor("i_lbp"));
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......"+tableName);
		return new HTable(conf,tableName);
	}
    private static void debug(Object obj) {
        System.out.println(String.format("### DEBUG: %s", obj.toString()));
    }    

	public void dropTable(String tableName) throws IOException {
		hbase.disableTable(tableName);
		hbase.deleteTable(tableName);
	}
	//public HTableInterface getOrCreateTable(String tableName, String... descriptors)
	public HTableInterface getOrCreateTable(String tableName)
			throws IOException {
		if (!tableExists(tableName)) {
//			doCreateTable(tableName, descriptors);
			doCreateTable(tableName);
		}
		return getTable(tableName);
	}
	
	public HTableInterface getTable(String tableName) {
		return tablePool.getTable(tableName);
	}

	public void insert(HTable table, String rowKey, List<String> values)
			throws IOException {
		if (values.size() == 3) {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(values.get(0)), Bytes.toBytes(values.get(1)),
					Bytes.toBytes(values.get(2)));
			table.put(put);
		}
	}
	/**
	 * 将图片插入到表中
	 * @param table 表名
	 * @param rowKey 图片文件名
	 * @param prefix 字符串content
	 * @param qualifier 空字符串
	 * @param value byte[]表示图片的字节流
	 * @throws IOException
	 */
	//public void insert(HTableInterface table, String rowKey, String prefix, String qualifier, byte[] value)
	public void insert(HTableInterface table, String rowKey, byte[] value1)
			throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("img_content"), null, value1);
		put.add(Bytes.toBytes("img_meta"), Bytes.toBytes("time"), Bytes.toBytes(System.currentTimeMillis()));
		put.add(Bytes.toBytes("img_meta"), Bytes.toBytes("type"), Bytes.toBytes("A"));
		System.out.println("before insert img value length:"+value1.length);
		table.put(put);
	}
	
	public boolean tableExists(String tableName) {
		try {
			return hbase.tableExists(tableName);
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return false;
	}

}