/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-13
 */
package com.sample.bulk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseBulkLoadDriver extends Configured implements Tool {	
    private static final String DATA_SEPERATOR = ",";	
    private static final String TABLE_NAME = "Myuser";	
    private static final String COLUMN_FAMILY_1="personalDetails";	
    private static final String COLUMN_FAMILY_2="contactDetails";	
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
    
	public static void createUserTable() throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists("Myuser")) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor("Myuser");
			// 在描述里添加列族
			tableDesc.addFamily(new HColumnDescriptor("personalDetails"));
			tableDesc.addFamily(new HColumnDescriptor("contactDetails"));
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表user成功");
		}
	}
    
    /**
     * HBase bulk import example
     * Data preparation MapReduce job driver
     * 
     * args[0]: HDFS input path
     * args[1]: HDFS output path
     * @throws Exception 
     * 
     */
    public static void main(String[] args) throws Exception {	
    	String[] myarg = new String[2];
    	myarg[0] = "hdfs://192.168.0.120:9000/user/hadoop/bkld";
    	myarg[1] = "hdfs://192.168.0.120:9000/user/hadoop/result";
        try {
            int response = ToolRunner.run(conf, new HBaseBulkLoadDriver(), myarg);			
            if(response == 0) {				
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int result=0;
        String outputPath = args[1];		
        Configuration configuration = getHBaseConf();		
        configuration.set("data.seperator", DATA_SEPERATOR);		
        configuration.set("hbase.table.name",TABLE_NAME);		
        configuration.set("COLUMN_FAMILY_1",COLUMN_FAMILY_1);		
        configuration.set("COLUMN_FAMILY_2",COLUMN_FAMILY_2);		
        Job job = new Job(configuration);		
        job.setJarByClass(HBaseBulkLoadDriver.class);
        job.setJobName("Bulk Loading HBase Table::"+TABLE_NAME);		
        job.setInputFormatClass(TextInputFormat.class);		
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);		
        job.setMapperClass(HBaseBulkLoadMapper.class);		
        FileInputFormat.addInputPaths(job, args[0]);		
        //FileSystem.getLocal(getConf()).delete(new Path(outputPath), true);	
         FileOutputFormat.setOutputPath(job, new Path(outputPath));		
        job.setMapOutputValueClass(Put.class);
        
        
        System.out.println("HFileOutputFormat.configureIncrementalLoad start...");
        HFileOutputFormat.configureIncrementalLoad(job, new HTable(configuration,TABLE_NAME));		
        job.waitForCompletion(true);
        System.out.println(" job.waitForCompletion(true)"+ job.waitForCompletion(true));
        
        
        
        if (job.isSuccessful()) {
            HbaseBulkLoad.doBulkLoad(outputPath, TABLE_NAME);
        } else {
            result = -1;
        }
        return result;
    }
}