package com.sample.aurora.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

	static class Mapper1 extends TableMapper<ImmutableBytesWritable, Writable> {


		@Override
		public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException {
			System.out.println("map row:" + new String(row.get()));

			// 这里将HTable的行键读出，处理以后得到新表AuroraDataTable的行键，对每条记录生成一个Put
			Put put = new Put(row.get());
			/**
			 * img的预处理算法
			 */
			try {
				// 计算出meta列族的数据，包括 时间，波段，观测模式 以及 预处理参数
				String timestr = new String(row.get()).substring(1, 7);
				String wave = "";
				String mode = "";
				// HashMap<String,String> preprocparam = new
				// HashMap<String,String>();
				JSONObject preprocparam = new JSONObject();// 包含暗电流，灰度校正系数，天顶中心，旋转角度，灰度映射

				preprocparam.put("darkCurrent", "");
				preprocparam.put("gammaCorrection", "");
				preprocparam.put("ZenithCenter", "");
				preprocparam.put("rotationAngle", "");
				preprocparam.put("greyMapping", "");

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

	// public static class Reducer1 extends TableReducer<ImmutableBytesWritable,
	// IntWritable, ImmutableBytesWritable> {
	//
	// public void reduce(ImmutableBytesWritable key, Iterable<IntWritable>
	// values, Context context)
	// throws IOException, InterruptedException {
	// int sum = 0;
	// for (IntWritable val : values) {
	// sum += val.get();
	// }
	//
	// Put put = new Put(key.get());
	// put.add(Bytes.toBytes("details"), Bytes.toBytes("total"),
	// Bytes.toBytes(sum));
	// System.out.println(String.format("stats :   key : %d,  count : %d",
	// Bytes.toInt(key.get()), sum));
	// context.write(key, put);
	// }
	// }

	public static void main(String[] args) throws Exception {
		// HBaseConfiguration conf = new HBaseConfiguration();
		Job job = new Job(configuration, "AuroraMR-job-preprocess");
		job.setJarByClass(PreProcessMR.class);
		Scan scan = new Scan();
		String columns = "raw"; 
		scan.addColumn(Bytes.toBytes(columns), null);
		scan.setFilter(new FirstKeyOnlyFilter());
		TableMapReduceUtil.initTableMapperJob("Aurora", scan,Mapper1.class, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("Aurora",IdentityTableReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
