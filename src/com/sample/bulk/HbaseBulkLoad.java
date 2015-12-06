/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-13
 */
package com.sample.bulk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HbaseBulkLoad {	
    /**
     * doBulkLoad.
     *
     * @param pathToHFile path to hfile
     * @param tableName 
     */
    public static void doBulkLoad(String pathToHFile, String tableName) {
        try {	
        	System.out.println("Bulk Load Start..");
            Configuration configuration = new Configuration();			
            configuration.set("mapreduce.child.java.opts", "-Xmx1g");	
            HBaseConfiguration.addHbaseResources(configuration);	
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);	
            HTable hTable = new HTable(configuration, tableName);
            System.out.println("Bulk%%%%%%%%%%%%%%%%%%%%%%%%..");
            loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);	
            System.out.println("Bulk Load Completed..");		
        } catch(Exception exception) {			
            exception.printStackTrace();			
        }		
    }	
}