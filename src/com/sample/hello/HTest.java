package com.sample.hello;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HTest {

	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.master", "192.168.137.135 :60000");
		configuration.set("hbase.zookeeper.quorum", "192.168.137.135");//配置Zookeeper集群的地址列表
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) throws IOException {
//		createTable("wujintao"); // 创建表
//		insertData("wujintao"); //增加数据
		QueryAll("wujintao"); //查询遍历数据
//		QueryByCondition1("wujintao"); //条件查询
//		QueryByCondition2("wujintao");
		//QueryByCondition3("wujintao");//have an exception
//		deleteRow("wujintao","112233bbbcccc"); //删除id为 112233bbbcccc 的数据
//		deleteByCondition("wujintao","abcdef");
	}

	/**
	 * 创建一张表
	 * 
	 * @param tableName
	 */
	public static void createTable(String tableName) {
		System.out.println("start create table ......"+tableName);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......"+tableName);
	}

	/**
	 * 增加一条数据
	 * 
	 * @param tableName
	 * @throws IOException
	 *             一个id 对应多个属性 ， 112233bbbcccc是id , column1是属性名字 ， aaa是属性 value
	 *             ， 和mysql一个意思
	 */
	public static void insertData(String tableName) throws IOException {
		System.out.println("start insert data to "+tableName+" ......");
		HTable table = new HTable(configuration, tableName);
		Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		put.add("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
		put.add("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第2列
		put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data to "+tableName+" ......");
	}

	public static void dropTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteRow(String tablename, String rowkey) {
		try {
			HTable table = new HTable(configuration, tablename);
			List list = new ArrayList();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteByCondition(String tablename, String rowkey) {
		// 目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作

	}

	/**
	 * 遍历查询
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void QueryAll(String tableName) throws IOException {
		 HTablePool pool = new HTablePool(configuration, 1000);
		 HTableInterface table = pool.getTable(tableName);
//		HTable table = new HTable(configuration, tableName);
		try {
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 条件查询 根据"112233bbbcccc" 进行条件查询
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void QueryByCondition1(String tableName) throws IOException {

		// HTablePool pool = new HTablePool(configuration, 1000);
		// HTable table = (HTable) pool.getTable(tableName);
		HTable table = new HTable(configuration, tableName);
		try {
			Get scan = new Get("112233bbbcccc".getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				System.out.println("列：" + new String(keyValue.getFamily())
						+ "====值:" + new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void QueryByCondition2(String tableName) {

		try {
			// HTablePool pool = new HTablePool(configuration, 1000);
			// HTable table = (HTable) pool.getTable(tableName);
			HTable table = new HTable(configuration, tableName);
			Filter filter = new SingleColumnValueFilter(
					Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void QueryByCondition3(String tableName) {

		try {
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(
					Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(
					Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(
					Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
