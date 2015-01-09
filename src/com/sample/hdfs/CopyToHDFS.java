package com.sample.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToHDFS {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path source = new Path("/home/hadoop/word.txt");
		Path dst = new Path("/user/root/njupt/");
		fs.copyFromLocalFile(source, dst);
	}
}
