package com.aurora.hbase.bulkload;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.GenericOptionsParser;


public class HFileLoader {

	public static void main(String[] args) throws Exception {
		String[] dfsArgs = new GenericOptionsParser(
				ConnectionUtil.getConfiguration(), args).getRemainingArgs();
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
				ConnectionUtil.getConfiguration());
		loader.doBulkLoad(new Path(dfsArgs[0]), ConnectionUtil.getTable());
	}

}