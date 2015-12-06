package com.aurora.hbase.mapreduce.preprocess;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.aroura.hbase.util.DataRead;
import com.aroura.hbase.util.HBaseJavaAPI;

/**
 * 
 * @brief 在极光数据集上运行MapReduce程序
 * @author huangpeng
 * @version 0.1
 * @date 2015-3-11 上午10:06:44
 * 
 */
public class PreProcessMR {
	private static Configuration configuration = HBaseJavaAPI.getHBaseConf();

	static class PreProcessMapper extends TableMapper<ImmutableBytesWritable, Writable> {


		@Override
		public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException {
			System.out.println("map row:" + new String(row.get()));

			// 这里将HTable的行键读出，处理以后得到新表AuroraDataTable的行键，对每条记录生成一个Put
			Put put = new Put(row.get());
			/**
			 * img的预处理算法
			 */
			try {
				KeyValue kv = HBaseJavaAPI.getDataByRowKey("Aurora",  new String(row.get()),"raw", "img");
				byte[] imgRaw = kv.getValue();
				
				File imgFile = DataRead.byteToFile(imgRaw);
				
				// 计算出meta列族的数据，包括 时间，波段，观测模式 以及 预处理参数
				String timestr = PreProcess.readTimeInfo(imgFile);
				String wave = PreProcess.waveband;
				String mode = PreProcess.mode;
				JSONObject preprocparam = new JSONObject();// 包含暗电流，灰度校正系数，天顶中心，旋转角度，灰度映射等

				preprocparam.put("noiseR", PreProcess.noiseR);
				preprocparam.put("noiseG", PreProcess.noiseG);
				preprocparam.put("noiseV", PreProcess.noiseV);
				preprocparam.put("LimRayleighR", PreProcess.LimRayleighR);
				preprocparam.put("LimRayleighG", PreProcess.LimRayleighG);
				preprocparam.put("LimRayleighV", PreProcess.LimRayleighV);
				preprocparam.put("Rx0", PreProcess.Rx0);
				preprocparam.put("Ry0", PreProcess.Ry0);
				preprocparam.put("Gx0", PreProcess.Gx0);
				preprocparam.put("Gy0", PreProcess.Gy0);
				preprocparam.put("Vx0", PreProcess.Vx0);
				preprocparam.put("Vy0", PreProcess.Vy0);
				preprocparam.put("RK", PreProcess.RK);
				preprocparam.put("GK", PreProcess.GK);
				preprocparam.put("VK", PreProcess.VK);
				preprocparam.put("rotatedegree", PreProcess.rotatedegree);
				

				put.add(Bytes.toBytes("meta"), Bytes.toBytes("time"),
						Bytes.toBytes(timestr));
				put.add(Bytes.toBytes("meta"), Bytes.toBytes("wave"),
						Bytes.toBytes(wave));
				put.add(Bytes.toBytes("meta"), Bytes.toBytes("mode"),
						Bytes.toBytes(mode));
				put.add(Bytes.toBytes("meta"), Bytes.toBytes("preprocparam"),
						preprocparam.toString().getBytes());

				System.out.println(String.format(
						"stats :   key : %s,  value i_meta:time : %s",
						new String(row.get()), timestr));
				
				
				// 将处理结果输出到新的表中
				context.write(row, put);
				 
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}


	public static void main(String[] args) throws Exception {
		// HBaseConfiguration conf = new HBaseConfiguration();
		Job job = new Job(configuration, "AuroraMR-job-preprocess");
		job.setJarByClass(PreProcessMR.class);
		Scan scan = new Scan();
		String family = "raw"; 
		String qualifier = "img";
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.setFilter(new FirstKeyOnlyFilter());
		TableMapReduceUtil.initTableMapperJob("Aurora", scan,PreProcessMapper.class, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("Aurora",IdentityTableReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
