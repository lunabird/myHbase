/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-7
 */
package com.aurora.hbase.mapreduce.bmp;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import javax.imageio.ImageIO;

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

/**
 * @author hadoop
 *
 */
public class GenerateBMPMR {
	private static Configuration configuration = HBaseJavaAPI.getHBaseConf();
	
	static class BMPMapper extends TableMapper<ImmutableBytesWritable, Writable> {


		@Override
		public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException {
			System.out.println("map row:" + new String(row.get()));

			// 这里将HTable的行键读出，处理以后得到新表AuroraDataTable的行键，对每条记录生成一个Put
			String rowkey = new String(row.get());
			String newRowkey = rowkey.substring(0, 2)+"2"+rowkey.substring(3);
			Put put = new Put(Bytes.toBytes(newRowkey));
			/**
			 * img的转换bmp算法
			 */
			try {
				// 计算出content列族的数据，包括 bmp位图和缩略图
				int targetW = 128;
				int targetH = 128;
				
				
				KeyValue kv = HBaseJavaAPI.getDataByRowKeyWithoutQualifier("HAuroraImgTable", new String(row.get()),"data");
				byte[] imgRaw = kv.getValue();
				
				File imgFile = DataRead.byteToFile(imgRaw);
				File fromFile=imgFile;
				
				
				BufferedImage source = ImageIO.read(fromFile);
				BufferedImage image = GenerateBMP.convertImgToBmp(fromFile);
//				BufferedImage thumbnail = Miniature.resize(source, targetW, targetH);

				

				put.add(Bytes.toBytes("data"), null,
						HBaseJavaAPI.bufferedImageToByte(image));
//				put.add(Bytes.toBytes("content"), Bytes.toBytes("thumbnail"),
//						HBaseJavaAPI.bufferedImageToByte(thumbnail));
				
				
				System.out.println(String.format("stats : key : %s",rowkey));
				
						
				
				// 将处理结果输出到新的表中
				ImmutableBytesWritable newRowkeyI = new ImmutableBytesWritable(Bytes.toBytes(newRowkey));
				context.write(newRowkeyI, put);
				 
			}catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	
	
	public static void main(String[] args) throws Exception {
		Job job = new Job(configuration, "AuroraMR-job-convertToBmp");
		job.setJarByClass(GenerateBMPMR.class);
		Scan scan = new Scan();
		String family = "data"; 
		String qualifier = "img";
		scan.addColumn(Bytes.toBytes(family),null);
		scan.setFilter(new FirstKeyOnlyFilter());
		TableMapReduceUtil.initTableMapperJob("HAuroraImgTable", scan,BMPMapper.class, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("HAuroraImgTable",null, job);
		long start = System.currentTimeMillis();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		long end = System.currentTimeMillis();
		//System.out.println("MR job total spend time:"+(end-start));
		writeToLogTxt("MR job total spend time:"+(end-start));
	}
		
	
	public static void writeToLogTxt(String a) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
					"Data\\time.txt"),true));
			writer.write(a+"\n");
			writer.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}