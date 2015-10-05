package com.aroura.hbase.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.sample.hdfs.CopyToHDFS;

/**
 * 操作hbase的工具类
 * 
 * @author hadoop
 * 
 */
public class HBaseJavaAPI {
	// 声明静态配置
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.master", "192.168.0.120:60000");
		conf.set("hbase.zookeeper.quorum", "192.168.0.120,192.168.0.121,192.168.0.122,192.168.0.123,192.168.0.124," +
				"192.168.0.125,192.168.0.126,192.168.0.127,192.168.0.128,192.168.0.129,192.168.0.130");//配置Zookeeper集群的地址列表
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	/**
	 * 创建数据库表
	 * 
	 * @param tableName-表名
	 * @param columnFamilys-列族数组
	 * @throws Exception
	 */
	public static void createTable(String tableName, String[] columnFamilys) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists(tableName)) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// 在描述里添加列族
			for (String columnFamily : columnFamilys) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表"+tableName+"成功");
		}
	}
	
	/**
	 * 创建数据库表Aurora
	 * 
	 * @throws Exception
	 */
	public static void createAuroraTable() throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists("Aurora")) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor("Aurora");
			// 在描述里添加列族
			tableDesc.addFamily(new HColumnDescriptor("raw").setBlocksize(589824));
			tableDesc.addFamily(new HColumnDescriptor("content").setBlocksize(589824));
			tableDesc.addFamily(new HColumnDescriptor("meta"));
			tableDesc.addFamily(new HColumnDescriptor("lbp"));
			tableDesc.addFamily(new HColumnDescriptor("keogram"));
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表Aurora成功");
		}
	}
	
	
	/**
	 * 删除数据库表
	 * @param tableName-hbase表名
	 * @throws Exception
	 */
	public static void deleteTable(String tableName) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists(tableName)) {
			// 关闭一个表
			hAdmin.disableTable(tableName);
			// 删除一个表
			hAdmin.deleteTable(tableName);
			System.out.println("删除表"+tableName+"成功");
		} else {
			System.out.println("删除的表"+tableName+"不存在");
			System.exit(0);
		}
	}

	/**
	 * 添加一条数据
	 * 
	 * @param tableName-表名
	 * @param rowKey-行键
	 * @param columnFamily-列族名
	 * @param column-列名
	 * @param value-值
	 * @throws Exception
	 */
	public static void addRow(String tableName, String rowKey, String columnFamily, String column, byte[] value) throws Exception {
		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		// 参数出分别：列族、列、值
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value);
		table.put(put);
	}

	/**
	 * 删除一条数据
	 * 
	 * @param tableName-表名
	 * @param rowKey-行键
	 * @throws Exception
	 */
	public static void delRow(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		Delete del = new Delete(Bytes.toBytes(rowKey));
		table.delete(del);
	}

	/**
	 * 删除多条数据
	 * @param tableName-表名
	 * @param rowKeys-行键数组
	 * @throws Exception
	 */
	public static void delMultiRows(String tableName, String[] rowKeys) throws Exception {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		for (String rowKey : rowKeys) {
			Delete del = new Delete(Bytes.toBytes(rowKey));
			list.add(del);
		}
		table.delete(list);
	}

	/**
	 * 获取一条数据
	 * @param tableName-表名
	 * @param rowKey-行键数组
	 * @throws Exception
	 */
	public static void getRow(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		// 输出结果
		for (KeyValue rowKV : result.raw()) {
			System.out.print("行名：" + new String(rowKV.getRow()) + " ");
			System.out.print("时间戳：" + rowKV.getTimestamp() + " ");
			System.out.print("列族名：" + new String(rowKV.getFamily()) + " ");
			System.out.print("列名：" + new String(rowKV.getQualifier()) + " ");
			System.out.println("值：" + new String(rowKV.getValue()) + " ");
		}
	}

	
	/**
	 * 条件查询 根据rowkey进行条件查询，将符合条件的bmp图片下载到本地。
	 * @param tableName 要查询的表名
	 * @param rowkey 要查询的行键
	 * @param outputPath 要下载该图片到本地的路径，是一个已经存在的目录。例如E:\
	 * @throws IOException
	 */
	public static void getBmpPictureByRowKey(String tableName,String rowkey,String outputPath) throws IOException {
		HTable table = new HTable(conf, tableName);
		try {
			Get scan = new Get(rowkey.getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				if(new String(keyValue.getQualifier()).equals("image")){
					FileUtils.writeByteArrayToFile(new File(outputPath+rowkey+".bmp"), keyValue.getValue());
				}
				System.out.println("列：" + new String(keyValue.getFamily())+new String(keyValue.getQualifier())
						+ "====值:" + new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 条件查询 根据rowkey进行条件查询，将符合条件的Thumbnail图片下载到本地。
	 * @param tableName
	 * @param rowkey
	 * @param outputPath
	 * @throws IOException
	 */
	public static void getThumbnailPictureByRowKey(String tableName,String rowkey,String outputPath) throws IOException {
		HTable table = new HTable(conf, tableName);
		try {
			Get scan = new Get(rowkey.getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				if(new String(keyValue.getQualifier()).equals("thumbnail")){
					FileUtils.writeByteArrayToFile(new File(outputPath+rowkey+"_thumb.bmp"), keyValue.getValue());
				}
				System.out.println("列：" + new String(keyValue.getFamily())+new String(keyValue.getQualifier())
						+ "====值:" + new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据rowkey获取对应的列的值
	 * @param tableName
	 * @param rowkey
	 * @param qualifier
	 * @throws IOException
	 */
	public static KeyValue getDataByRowKey(String tableName,String rowkey,String qualifier) throws IOException {
		HTable table = new HTable(conf, tableName);
		try {
			Get scan = new Get(rowkey.getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				if(new String(keyValue.getQualifier()).equals(qualifier)){
					System.out.println("列：" + new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())
					+ "====值:" + new String(keyValue.getValue()));
					return keyValue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 查询所有列值为queryString的行
	 * @param tableName
	 * @param colFamily
	 * @param qualifier
	 * @param queryString
	 */
	public static void QueryByCondition2(String tableName,String colFamily,String qualifier,String queryString) {
		try {
			HTable table = new HTable(conf, tableName);
			// 当列的值为queryString时进行查询
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,Bytes.toBytes(queryString)); 
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())+new String(keyValue.getQualifier())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	/**
	 * 获取所有数据
	 * @param tableName-表名
	 * @throws Exception
	 */
	public static void getAllRows(String tableName) throws Exception {
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		//scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("attr"));
		//scan.setStartRow( Bytes.toBytes("row"));                   // start key is inclusive
		// scan.setStopRow( Bytes.toBytes("row" + (char)0)); // stop key is  exclusive
		// 输出结果
		for (Result result : results) {
			//for (Result r = rs.next(); r != null; r = rs.next()) {
			for (KeyValue rowKV : result.raw()) {
				System.out.print("行名：" + new String(rowKV.getRow()) + " ");
				System.out.print("时间戳：" + rowKV.getTimestamp() + " ");
				System.out.print("列族名：" + new String(rowKV.getFamily()) + " ");
				System.out.print("列名：" + new String(rowKV.getQualifier()) + " ");
				System.out.println("值：" + new String(rowKV.getValue()) + " ");
			}
		}
	}
	/**
	 * 获取指定范围内的记录
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @param family
	 * @param qualifier
	 * @throws Exception
	 */
	public static void getSelectRows(String tableName,String startRow,String stopRow,String family,String qualifier) throws Exception {
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));
		scan.setStartRow( Bytes.toBytes(startRow));                   // start key is inclusive
		scan.setStopRow( Bytes.toBytes(stopRow + (char)0)); // stop key is  exclusive
		// 输出结果
		for (Result result : results) {
			for (KeyValue rowKV : result.raw()) {
				System.out.print("行名：" + new String(rowKV.getRow()) + " ");
				System.out.print("时间戳：" + rowKV.getTimestamp() + " ");
				System.out.print("列族名：" + new String(rowKV.getFamily()) + " ");
				System.out.print("列名：" + new String(rowKV.getQualifier()) + " ");
				System.out.println("值：" + new String(rowKV.getValue()) + " ");
			}
		}
	}
	
	
	// 主函数
	public static void main(String[] args) {
		try {
			/*
			String tableName = "Aurora";
			// 第一步：创建数据库表：“student”
			String[] columnFamilys = { "raw", "content","meta","lbp","keogram" };
			HBaseJavaAPI.createTable(tableName, columnFamilys);
			*/
			
			
			//获取数据库表
			HTable Table = new HTable(conf, "Aurora");
			
			/*
			// 第二步：向数据表的添加数据
			ArrayList<String> list = CopyToHDFS.readLogTxt();
			for(int i=0;i<list.size();i++){
				String sourcePath = list.get(i);
				String[] tmp = sourcePath.split("\\\\");
				String targetPath = "/home/hp/Desktop/N20041109G_F/"+tmp[tmp.length-1];
				String fileName = targetPath;
				File file = new File(fileName);
				byte[] content = FileUtils.readFileToByteArray(file);//将图片内容转换成字节流
				HBaseJavaAPI.addRow("Aurora", tmp[tmp.length-1], "raw", "img", content);
				System.out.println(targetPath+"  loaded successfully!");
			}
			System.out.println("done");
			*/
			
			
			
			/*
			
			// 第三步：获取一条数据
			System.out.println("获取一条数据");
			HBaseJavaAPI.getRow(tableName, "xiapi");
			// 第四步：获取所有数据
			System.out.println("获取所有数据");
			HBaseJavaAPI.getAllRows(tableName);
			// 第五步：删除一条数据
			System.out.println("删除一条数据");
			HBaseJavaAPI.delRow(tableName, "xiapi");
			HBaseJavaAPI.getAllRows(tableName);
			// 第六步：删除多条数据
			System.out.println("删除多条数据");
			String[] rows = { "xiaoxue", "qingqing" };
			HBaseJavaAPI.delMultiRows(tableName, rows);
			HBaseJavaAPI.getAllRows(tableName);
			// 第八步：删除数据库
			System.out.println("删除数据库");
			HBaseJavaAPI.deleteTable(tableName);
			*/
			
			
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}