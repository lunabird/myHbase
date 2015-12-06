package com.aurora.hbase.bulkload;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class HFileGenerator {
	
	private static final String[] DATA = {
	    "One, two, buckle my shoe",
	    "Three, four, shut the door",
	    "Five, six, pick up sticks",
	    "Seven, eight, lay them straight",
	    "Nine, ten, a big fat hen"
	  };
	  
	public static void sqcFile() throws IOException {
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
	
	private static SequenceFile.Reader reader = null;  
   // private static Configuration conf = new Configuration();  
  
    public static class ReadFileMapper extends  
            Mapper<LongWritable, Text,  ImmutableBytesWritable, KeyValue> {  
  
        /* (non-Javadoc) 
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context) 
         */  
        @Override  
        public void map(LongWritable key, Text value, Context context) {  
        	Configuration conf = new Configuration();  
            key = (LongWritable) ReflectionUtils.newInstance(  
                    reader.getKeyClass(), conf);  
            value = (Text) ReflectionUtils.newInstance(  
                    reader.getValueClass(), conf);  
			// 创建HBase中的RowKey
			byte[] rowKey = Bytes.toBytes(key.toString());
			ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(
					rowKey);
			byte[] family = Bytes.toBytes("cf");
			byte[] qualifier = Bytes.toBytes("count");
			byte[] hbaseValue = Bytes.toBytes(value.toString());
			// Put 用于列簇下的多列提交，若只有一个列，则可以使用 KeyValue 格式
			KeyValue keyValue = new KeyValue(rowKey, family, qualifier,
					hbaseValue);
            try {  
                while (reader.next(key, value)) {  
                    System.out.printf("%s\t%s\n", key, value);  
                    context.write(rowKeyWritable, keyValue);  
                }  
            } catch (IOException e1) {  
                e1.printStackTrace();  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
        }  
  
    }  
    
	
	
	
	
/*
	public static class HFileMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split(",", -1);
			ImmutableBytesWritable rowkey = new ImmutableBytesWritable(
					items[0].getBytes());

			//new KeyValue(byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value)
			
			KeyValue kv = new KeyValue(Bytes.toBytes(items[0]),
					Bytes.toBytes(items[1]), Bytes.toBytes(items[2]),
					System.currentTimeMillis(), Bytes.toBytes(items[3]));
			if (null != kv) {
				context.write(rowkey, kv);
			}
			// TODO Auto-generated method stub
			String wordCountStr = value.toString();
			String[] wordCountArray = wordCountStr.split("\t");
			String word = wordCountArray[0];
			int count = Integer.valueOf(wordCountArray[1]);

			// 创建HBase中的RowKey
			byte[] rowKey = Bytes.toBytes(word);
			ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(
					rowKey);
			byte[] family = Bytes.toBytes("cf");
			byte[] qualifier = Bytes.toBytes("count");
			byte[] hbaseValue = Bytes.toBytes(count);
			// Put 用于列簇下的多列提交，若只有一个列，则可以使用 KeyValue 格式
			KeyValue keyValue = new KeyValue(rowKey, family, qualifier,hbaseValue);
//			Put put = new Put(rowKey);
//			put.add(family, qualifier, hbaseValue);
			context.write(rowKeyWritable, keyValue);

		}
	}
	*/

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		//sqcFile();
		/*
		Job job = new Job(conf,"read seq file");  
        job.setJarByClass(HFileGenerator.class);  
        job.setMapperClass(ReadFileMapper.class);  
        job.setMapOutputValueClass(Text.class);  
        Path inputPath = new Path("hdfs://192.168.0.120:9000/user/hadoop/huangpeng/other/sqcFile");
		inputPath = inputPath.makeQualified(FileSystem.get(job.getConfiguration()));
        FileSystem fs = FileSystem.get(conf);  
        reader = new SequenceFile.Reader(fs, inputPath, conf);  
        FileInputFormat.addInputPath(job, inputPath);  
        FileOutputFormat.setOutputPath(job, new Path("hdfs:///192.168.0.120:9000/user/hadoop/huangpeng/other/result"));  
        System.exit(job.waitForCompletion(true)?0:1); 
		*/
		
		//
//		String[] dfsArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
		
		
		
		String[] dfsArgs = new String[2];
		dfsArgs[0] = "hdfs://192.168.0.120:9000/user/hadoop/huangpeng/other/sqcFile";
		dfsArgs[1] = "hdfs://192.168.0.120:9000/user/hadoop/huangpeng/other/result"; 
		
		
		
		Job job = new Job(conf, "HFile bulk load test");
		job.setJarByClass(HFileGenerator.class);

		job.setMapperClass(ReadFileMapper.class);
		job.setReducerClass(KeyValueSortReducer.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		//job.setMapOutputValueClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

		Path inputPath = new Path(dfsArgs[0]);
		inputPath = inputPath.makeQualified(FileSystem.get(job.getConfiguration()));
		Path outputPath = new Path(dfsArgs[1]);
		outputPath = outputPath.makeQualified(FileSystem.get(job.getConfiguration()));
		
		System.out.println("inputPath="+inputPath);
		System.out.println("outputPath="+outputPath);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		//Path partitionFile = new Path(inputPath);  
		Path partitionFile = inputPath;
		System.out.println("Partition file path: " + inputPath);  
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile); 
		
		

		HFileOutputFormat.configureIncrementalLoad(job,
				ConnectionUtil.getTable());
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}