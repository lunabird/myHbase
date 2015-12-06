/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-27
 */
package com.sample.bulk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author hadoop
 * 
 */
public class FromLunWen {

	static class TestHFileToHBaseMapper extends Mapper {

		protected void map(Text key, BytesWritable value,Context context) throws IOException, InterruptedException {

			ImmutableBytesWritable k = new ImmutableBytesWritable(key.toString().getBytes());

			byte[] contents = value.getBytes();

			int len = contents.length;

			Put putValue = new Put();

			putValue.add("data".getBytes(), null, contents);

			context.write(k, putValue);

		}

	}

	// 生成HFile的MapReduce的作业配置代码
	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException {

		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "TestHFileToHBase");
		
		// job.setJarByClass(TestHFileToHBase.class);
		job.setJarByClass(FromLunWen.class);

		job.setInputFormatClass(WholeFilelnputFormat.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);

		job.setMapOutputValueClass(Put.class);

		job.setMapperClass(TestHFileToHBaseMapper.class);

		job.setReducerClass(PutSortReducer.class);

		job.setOutputFormatClass(HFileOutputFormat.class);

		HFileOutputFormat.configureIncrementalLoad(job, new HTable(conf,"HAuroraImgTable"));

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}


