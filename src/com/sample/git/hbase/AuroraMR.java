package com.sample.git.hbase;

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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;



public class AuroraMR {
	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.master", "192.168.137.135 :60000");
		configuration.set("hbase.zookeeper.quorum", "192.168.137.135");//配置Zookeeper集群的地址列表
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
    static class Mapper1 extends TableMapper<ImmutableBytesWritable, Writable> {

//        private int numRecords = 0;
//        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            // extract userKey from the compositeKey (userId + counter)
        	System.out.println("map row:"+new String(row.get()));
//        	ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get());
//          ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
        	
        	//这里将RawImgTable的行键读出，处理以后得到新表AuroraDataTable的行键，对每条记录生成一个Put
        	Put put = new Put(row.get());
        	/**
        	 * img的预处理算法
        	 */
        	String timestr = new String(row.get()).substring(1, 7);
        	put.add(Bytes.toBytes("i_meta"), Bytes.toBytes("time"), Bytes.toBytes(timestr));
        	System.out.println(String.format("stats :   key : %s,  value i_meta:time : %s", new String(row.get()), timestr));
        	//将处理结果输出到新的表中
            try {
				context.write(row, put);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        
//            try {
//                context.write(userKey, one);
//            } catch (InterruptedException e) {
//                throw new IOException(e);
//            }
//            numRecords++;
//            if ((numRecords % 10000) == 0) {
//                context.setStatus("mapper processed " + numRecords + " records so far");
//            }
        }
    }

//    public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
//
//        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
//                throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//
//            Put put = new Put(key.get());
//            put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
//            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
//            context.write(key, put);
//        }
//    }
    
    public static void main(String[] args) throws Exception {
//        HBaseConfiguration conf = new HBaseConfiguration();
        Job job = new Job(configuration, "AuroraMR-job");
        job.setJarByClass(AuroraMR.class);
        Scan scan = new Scan();
        String columns = "img_content"; // 
        scan.addColumn(Bytes.toBytes(columns),null);
        scan.setFilter(new FirstKeyOnlyFilter());
        TableMapReduceUtil.initTableMapperJob("RawImgTable", scan, Mapper1.class, ImmutableBytesWritable.class,
                Put.class, job);
        TableMapReduceUtil.initTableReducerJob("AuroraDataTable", IdentityTableReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
