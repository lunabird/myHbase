package com.aurora.hbase.bulkload;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 	比如原始的输入数据的目录为：/rawdata/test/wordcount/20131212 
 * 	中间结果数据保存的目录为：/middata/test/wordcount/20131212 
 * 	最终生成的HFile保存的目录为：/resultdata/test/wordcount/20131212 
 * 	运行上面的Job的方式如下： 
 * 	hadoop jar test.jar /rawdata/test/wordcount/20131212 /middata/test/wordcount/20131212 /resultdata/test/wordcount/20131212 
 * 
 * @author hadoop
 *
 */

public class GeneratePutHFileAndBulkLoadToHBase {
	
	
	
	
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
		hBaseConfiguration.set("mapred.job.tracker", "192.168.0.120:9001");
	}

//	private static HBaseAdmin hbase;
//	private static final HTablePool tablePool = new HTablePool(hBaseConfiguration, Integer.MAX_VALUE);
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text wordText = new Text();
		private IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] wordArray = line.split(" ");
			for (String word : wordArray) {
				wordText.set(word);
				context.write(wordText, one);
			}

		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> valueList,Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/*int sum = 0;
			for (IntWritable value : valueList) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);*/
			
			
			int sum = 0;
			for (IntWritable value : valueList) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
			
			
		}

	}
	/**
	 * 
	 * @author hadoop
	 *
	 */
	public static class ConvertWordCountOutToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String wordCountStr = value.toString();
			String[] wordCountArray = wordCountStr.split("\t");
			String word = wordCountArray[0];
			int count = Integer.valueOf("100");

			// 创建HBase中的RowKey
			byte[] rowKey = Bytes.toBytes(word);
			ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowKey);
			byte[] family = Bytes.toBytes("cf");
			byte[] qualifier = Bytes.toBytes("count");
			byte[] hbaseValue = Bytes.toBytes(count);
			// Put 用于列簇下的多列提交，若只有一个列，则可以使用 KeyValue 格式
			// KeyValue keyValue = new KeyValue(rowKey, family, qualifier,
			// hbaseValue);
			Put put = new Put(rowKey);
			put.add(family, qualifier, hbaseValue);
			context.write(rowKeyWritable, put);

		}

	}

	public static HTable createWordCountTable(Configuration conf) throws IOException{
		String tableName = "word_count";
		System.out.println("start create table ......word_count");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			//第一个列簇
			tableDescriptor.addFamily(new HColumnDescriptor("cf"));
			
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
	
	
		
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration hadoopConfiguration = new Configuration();
		hadoopConfiguration.set("fs.default.name", "hdfs://192.168.0.120:9000"); 
		
		
		
//		hadoopConfiguration.set("mapred.job.tracker","192.168.75.130:9001"); 
//		hadoopConfiguration.get("mapred.job.tracker"); 
		
		//String[] dfsArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs();
		
		String[] dfsArgs = new String[3];
		dfsArgs[0] = "hdfs://192.168.0.120:9000/user/hadoop/wordCountInputFile";
		dfsArgs[1] = "hdfs://192.168.0.120:9000/user/hadoop/mid";
		dfsArgs[2] = "hdfs://192.168.0.120:9000/user/hadoop/result";
		
		// 第一个Job就是普通MR，输出到指定的目录
		/*
		Job job = new Job(hadoopConfiguration, "wordCountJob");
		job.setJarByClass(GeneratePutHFileAndBulkLoadToHBase.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(dfsArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(dfsArgs[1]));
		// 提交第一个Job
		int wordCountJobResult = job.waitForCompletion(true) ? 0 : 1;
		*/
		
		
		
		// 第二个Job以第一个Job的输出做为输入，只需要编写Mapper类，在Mapper类中对一个job的输出进行分析，并转换为HBase需要的KeyValue的方式。
		Job convertWordCountJobOutputToHFileJob = new Job(hadoopConfiguration,
				"wordCount_bulkload");

		convertWordCountJobOutputToHFileJob
				.setJarByClass(GeneratePutHFileAndBulkLoadToHBase.class);
		convertWordCountJobOutputToHFileJob
				.setMapperClass(ConvertWordCountOutToHFileMapper.class);
		// ReducerClass 无需指定，框架会自行根据 MapOutputValueClass 来决定是使用
		// KeyValueSortReducer 还是 PutSortReducer
		// convertWordCountJobOutputToHFileJob.setReducerClass(KeyValueSortReducer.class);
		convertWordCountJobOutputToHFileJob
				.setMapOutputKeyClass(ImmutableBytesWritable.class);
		convertWordCountJobOutputToHFileJob.setMapOutputValueClass(Put.class);

		// 以第一个Job的输出做为第二个Job的输入
		FileInputFormat.addInputPath(convertWordCountJobOutputToHFileJob,
				new Path("hdfs://192.168.0.120:9000/user/hadoop/partitions_90a33052-b2f6-4214-b00d-1b84fe47f1ab"));
		FileOutputFormat.setOutputPath(convertWordCountJobOutputToHFileJob,
				new Path(dfsArgs[2]));
		Path partitionFile = new Path( "hdfs://192.168.0.120:9000/user/hadoop/partitions_90a33052-b2f6-4214-b00d-1b84fe47f1ab");
		URI partitionUri = new URI(partitionFile.toString() +"#_partitions");
		
		TotalOrderPartitioner.setPartitionFile(convertWordCountJobOutputToHFileJob.getConfiguration(),partitionFile);
		// 创建目标表对象
		HTable wordCountTable = new HTable(hBaseConfiguration, "word_count");
		HFileOutputFormat.configureIncrementalLoad(convertWordCountJobOutputToHFileJob, wordCountTable);
		System.out.println("************************************************");
		
		
		// 提交第二个job
		int convertWordCountJobOutputToHFileJobResult = convertWordCountJobOutputToHFileJob.waitForCompletion(true) ? 0 : 1;
		System.out.println("convertWordCountJobOutputToHFileJobResult="+convertWordCountJobOutputToHFileJobResult);
		
		
		
		
		// 当第二个job结束之后，调用BulkLoad方式来将MR结果批量入库
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hBaseConfiguration);
		// 第一个参数为第二个Job的输出目录即保存HFile的目录，第二个参数为目标表
		loader.doBulkLoad(new Path(dfsArgs[2]), wordCountTable);

		// 最后调用System.exit进行退出
		System.exit(convertWordCountJobOutputToHFileJobResult);
		 
	}
	
	/*public static HTableInterface getOrCreateTable(String tableName)
			throws IOException {
		if (!tableExists(tableName)) {
//			doCreateTable(tableName, descriptors);
			createWordCountTable(hBaseConfiguration);
		}
		return getTable(tableName);
	}
	public static boolean tableExists(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException {
		hbase = new HBaseAdmin(hBaseConfiguration);
		
		try {
			return hbase.tableExists("word_count");
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return false;
	}
	public static HTableInterface getTable(String tableName) {
		return tablePool.getTable(tableName);
	}*/
	
	
	private static final String[] DATA = {
	    "One, two, buckle my shoe",
	    "Three, four, shut the door",
	    "Five, six, pick up sticks",
	    "Seven, eight, lay them straight",
	    "Nine, ten, a big fat hen"
	  };
	  
	public static void sqcFile(String[] args) throws IOException {
		String uri = "hdfs://192.168.0.120:9000/user/hadoop/huangpeng/other/sqcFile";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
					value.getClass());// 返回一个SequenceFile.Writer实例 需要数据流和path对象
										// 将数据写入了path对象

			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,
						value);// getLength（）方法获取的是当前文件的读取位置
								// 在这个位置开始写
				writer.append(key, value);// 将每条记录追加到SequenceFile.Writer实例的末尾
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
}