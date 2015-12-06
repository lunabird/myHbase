package com.aroura.hbase.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.imageio.ImageIO;

import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.aurora.hbase.hdfs.CopyToHDFS;

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

	public static Configuration getHBaseConf(){
		return conf;
	}
	/**
	 * 创建数据库表
	 * 
	 * @param tableName-表名
	 * @param columnFamilys-列族数组
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void createTable(String tableName, String[] columnFamilys) throws IOException {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists(tableName)) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// 在描述里添加列族
			for (String columnFamily:columnFamilys) {
//				HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
//				hcd.setCompressionType(Algorithm.SNAPPY);
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表"+tableName+"成功");
		}
	}
	
	/**
	 * 创建数据库表HAuroraImgTable
	 * 
	 * @throws Exception
	 */
	public static void createAuroraImgTable() throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists("HAuroraImgTable")) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor("HAuroraImgTable");
			// 在描述里添加列族
			tableDesc.addFamily(new HColumnDescriptor("data").setBlocksize(589824));
			tableDesc.addFamily(new HColumnDescriptor("metas"));
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表HAuroraImgTable成功");
		}
	}
	public static void createHAuroraDataTable() throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists("HAuroraDataTable")) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor("HAuroraDataTable");
			// 在描述里添加列族
			tableDesc.addFamily(new HColumnDescriptor("metas").setBlocksize(589824));
			tableDesc.addFamily(new HColumnDescriptor("algorithm"));
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表HAuroraDataTable成功");
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
	public static void addRow(String tableName, String rowKey, String columnFamily, byte[] value) throws Exception {
		HTable table = new HTable(conf, tableName);
		table.setAutoFlush(true);
        table.setWriteBufferSize(1024*1024*24);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.setWriteToWAL(false);
		// 参数出分别：列族、列、值
		
		put.add(Bytes.toBytes(columnFamily), null, value);
		
		table.put(put);
	}

	/**
	 * 向表中插入数据
	 * @param tableName
	 * @param columnFamily
	 * @throws IOException
	 */
	public static void setAutoFlushTest(String tableName,String columnFamily) throws IOException{
		HTable table = new HTable(conf, tableName);
		
		table.setAutoFlush(false);
        table.setWriteBufferSize(60*1024*1024);//
        
		ArrayList<String> list = CopyToHDFS.readLogTxt();	
		
		List<Put> lp = new ArrayList<Put>();  
		for(int i=0;i<list.size();i++){
			String sourcePath = list.get(i);
			String[] tmp = sourcePath.split("\\\\");
			String targetPath = "E:/AuroraRawData-2004/200411/N20041109G_F/"+tmp[tmp.length-1];
			String fileName = targetPath;
			System.out.println("fileName:"+fileName);
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
			put.add(Bytes.toBytes(columnFamily), null, content);
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
	}
	/**
	 * 删除一条数据
	 * 
	 * @param tableName-表名
	 * @param rowKey-行键
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void delRow(String tableName, String rowKey) throws IOException{
		HTable table = new HTable(conf, tableName);
		Delete del = new Delete(Bytes.toBytes(rowKey));
		table.delete(del);
	}

	/**
	 * 删除多条数据
	 * @param tableName-表名
	 * @param rowKeys-行键数组
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void delMultiRows(String tableName, String[] rowKeys) throws IOException {
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
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void getRow(String tableName, String rowKey) throws IOException {
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
	public static KeyValue getDataByRowKey(String tableName,String rowkey,String family,String qualifier) throws IOException {
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
	public static KeyValue getDataByRowKeyWithoutQualifier(String tableName,String rowkey,String family) throws IOException {
		HTable table = new HTable(conf, tableName);
		try {
			Get get = new Get(rowkey.getBytes());// 根据rowkey查询
			Result r = table.get(get);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				if(new String(keyValue.getFamily()).equals(family)){
					System.out.println("列：" + new String(keyValue.getFamily()));
					return keyValue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	/** QueryByCondition2(String tableName,String colFamily,String qualifier,String queryString):void
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
				System.out.println("行名：" + new String(rowKV.getRow()) + " ");
//				System.out.print("时间戳：" + rowKV.getTimestamp() + " ");
//				System.out.print("列族名：" + new String(rowKV.getFamily()) + " ");
//				System.out.print("列名：" + new String(rowKV.getQualifier()) + " ");
//				System.out.println("值：" + new String(rowKV.getValue()) + " ");
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
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void getSelectRows(String tableName,String startRow,String stopRow,String family,String qualifier) throws IOException {
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
	/** 
	 * 将BufferedImage转化为byte[]
	 * @param bi
	 * @return
	 * @throws IOException
	 */
	public static byte[] bufferedImageToByte(BufferedImage bi) throws IOException {

		BufferedImage originalImage = bi;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(originalImage, "bmp", baos);
		baos.flush();
		byte[] imageInByte = baos.toByteArray();
		baos.close();
		return imageInByte;

	}
	/**
	 * 获取表中的所有行键
	 * @param tableName
	 * @throws IOException
	 */
	public void getLevelRowKeys(String tableName,String level) throws IOException{
		ArrayList<String> rowkeyList = new ArrayList<String>();
		HTable table = new HTable(conf, tableName.getBytes());
		System.out.println("scanning full table:");
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		ResultScanner scanner = table.getScanner(scan);
		for (Result rr : scanner) {
//		  byte[] key = rr.getRow();
		  rowkeyList.add(new String(rr.getRow()));
		}
	}
	
	public void getSelectedRowKeys(String tableName,String dateStr) throws IOException{
		ArrayList<String> rowkeyList = new ArrayList<String>();
		HTable table = new HTable(conf, tableName.getBytes());
		System.out.println("scanning full table:");
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		ResultScanner scanner = table.getScanner(scan);
		for (Result rr : scanner) {
//		  byte[] key = rr.getRow();
		  rowkeyList.add(new String(rr.getRow()));
		}
	}
	
	// 主函数
	public static void main(String[] args) {
		try {
			
			deleteTable("HAuroraImgTable");
			createAuroraImgTable();
			
			//获取数据库表
			//HTable table = new HTable(conf, "HAuroraImgTable");
			
			
			// 第二步：向数据表的添加数据
			long all = System.currentTimeMillis();
			setAutoFlushTest("HAuroraImgTable","data");
			long end = System.currentTimeMillis();
	        System.out.println("total need time = "+ (end - all)*1.0/1000+"s");
	        System.out.println("insert complete"+",costs:"+(end - all)+"ms");
			
			
			
			
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