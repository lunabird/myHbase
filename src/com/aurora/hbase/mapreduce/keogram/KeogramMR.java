/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-7
 */
package com.aurora.hbase.mapreduce.keogram;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

import com.aroura.hbase.util.DataRead;
import com.aroura.hbase.util.HBaseJavaAPI;
import com.aurora.hbase.mapreduce.bmp.GenerateBMP;

/**
 * @author hadoop
 *  FileOutputFormat.setOutputPath(JobConf, Path) 用来设置文件输出到hdfs上
 */
public class KeogramMR {
	private static Configuration configuration = HBaseJavaAPI.getHBaseConf();
	
	static class KeogramMapper extends TableMapper<ImmutableBytesWritable, Writable> {


		@Override
		public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException {
			System.out.println("map row:" + new String(row.get()));

			// 这里将HTable的行键读出，处理以后得到新表AuroraDataTable的行键，对每条记录生成一个Put
			Put put = new Put(row.get());
			try {
				// 计算出content列族的数据，包括 bmp位图和缩略图
				
				
				KeyValue kv = HBaseJavaAPI.getDataByRowKey("Aurora",  new String(row.get()),"content", "image");
				byte[] imgRaw = kv.getValue();
				
				File imgFile = DataRead.byteToFile(imgRaw);
				File fromFile= imgFile;
				
				int[] keo_data = ChooseKeodata.chooseData(fromFile);

				put.add(Bytes.toBytes("keogram"), Bytes.toBytes("intensity"),
						integersToBytes(keo_data));

				System.out.println(String.format("stats : key : %s",new String(row.get())));
				
				
				// 将处理结果输出到新的表中
				context.write(row, put);
				 
			}catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/**
	 * 将int数组转为byte数组
	 * @param values
	 * @return
	 */
	public static byte[] integersToBytes(int[] values)
	{
	   ByteArrayOutputStream baos = new ByteArrayOutputStream();
	   DataOutputStream dos = new DataOutputStream(baos);
	   for(int i=0; i < values.length; ++i)
	   {
	        try {
				dos.writeInt(values[i]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   }

	   return baos.toByteArray();
	}
	
	
	public static void main(String[] args) throws Exception {
		Job job = new Job(configuration, "AuroraMR-job-keogram");
		job.setJarByClass(KeogramMR.class);
		Scan scan = new Scan();
		String family = "content"; 
		String qualifier = "image";
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.setFilter(new FirstKeyOnlyFilter());
		TableMapReduceUtil.initTableMapperJob("Aurora", scan,KeogramMapper.class, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("Aurora",IdentityTableReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
}
