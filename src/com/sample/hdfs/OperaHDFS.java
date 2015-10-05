/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-2
 */
package com.sample.hdfs;

/**
 * @author hadoop
 *
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OperaHDFS {

	public static void main(String[] args) throws Exception {

		// System.out.println("aaa");
		//uploadFile();
		// createFileOnHDFS();
		// deleteFileOnHDFS();
		// createDirectoryOnHDFS();
		// deleteDirectoryOnHDFS();
		// renameFileOrDirectoryOnHDFS();
		// downloadFileorDirectoryOnHDFS();
		// readHDFSListAll();
		
		readFromHdfs();
	}
	/***
	 * 加载配置文件
	 * **/
	static Configuration conf = new Configuration();
	
	private static void readFromHdfs() throws FileNotFoundException,
			IOException {

		String dst = "hdfs://192.168.0.120:9000/user/hadoop/huangpeng/picture/N041109G00001.img";
		FileSystem fs = FileSystem.get(conf);
		//Configuration conf = new Configuration();

		//FileSystem fs = FileSystem.get(URI.create(dst), conf);

		FSDataInputStream hdfsInStream = fs.open(new Path(dst));

		OutputStream out = new FileOutputStream("/home/hp/Desktop/N041109G00001.img");

		byte[] ioBuffer = new byte[1024];

		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {

			out.write(ioBuffer, 0, readLen);

			readLen = hdfsInStream.read(ioBuffer);

		}

		out.close();

		hdfsInStream.close();

		fs.close();

	}
	/***
	 * 上传windows本地文件到 HDFS上
	 * 
	 * **/
	public static void uploadFile() throws Exception {
		conf.addResource(new Path("/home/hadoop-1.0.4/conf/core-site.xml"));
		conf.addResource(new Path("/home/hadoop-1.0.4/conf/hdfs-site.xml"));
		// 加载默认配置
		FileSystem fs = FileSystem.get(conf);
		// 本地文件
		Path src = new Path("D:\\auroramatlab\\info.txt");
		// HDFS为止
		Path dst = new Path(
				"file:///192.168.0.120:9000/user/hadoop/huangpeng/picture/info.txt");
		try {
			fs.copyFromLocalFile(src, dst);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("上传成功........");

		fs.close();// 释放资源

	}
	
	

	/**
	 * 重名名一个文件夹或者文件
	 * 
	 * **/
	public static void renameFileOrDirectoryOnHDFS() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path p1 = new Path("hdfs://10.2.143.5:9090/root/myfile/my.txt");
		Path p2 = new Path("hdfs://10.2.143.5:9090/root/myfile/my2.txt");
		fs.rename(p1, p2);

		fs.close();// 释放资源
		System.out.println("重命名文件夹或文件成功.....");

	}

	/***
	 * 
	 * 读取HDFS某个文件夹的所有 文件，并打印
	 * 
	 * **/
	public static void readHDFSListAll() throws Exception {

		// 流读入和写入
		InputStream in = null;
		// 获取HDFS的conf
		// 读取HDFS上的文件系统
		FileSystem hdfs = FileSystem.get(conf);
		// 使用缓冲流，进行按行读取的功能
		BufferedReader buff = null;
		// 获取日志文件的根目录
		// Path listf =new Path("hdfs://10.2.143.5:9090/root/myfile/");
		Path listf = new Path(
				"hdfs:///192.168.0.120:9000/user/hadoop/huangpeng/picture/");
		// 获取根目录下的所有2级子文件目录
		FileStatus stats[] = hdfs.listStatus(listf);
		// 自定义j，方便查看插入信息
		int j = 0;
		for (int i = 0; i < stats.length; i++) {
			// 获取子目录下的文件路径
			FileStatus temp[] = hdfs.listStatus(new Path(stats[i].getPath()
					.toString()));
			for (int k = 0; k < temp.length; k++) {
				System.out.println("文件路径名:" + temp[k].getPath().toString());
				// 获取Path
				Path p = new Path(temp[k].getPath().toString());
				// 打开文件流
				in = hdfs.open(p);
				// BufferedReader包装一个流
				buff = new BufferedReader(new InputStreamReader(in));
				String str = null;
				while ((str = buff.readLine()) != null) {

					System.out.println(str);
				}
				buff.close();
				in.close();

			}

		}

		hdfs.close();

	}

	/**
	 * 从HDFS上下载文件或文件夹到本地
	 * 
	 * **/
	public static void downloadFileorDirectoryOnHDFS() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path p1 = new Path("hdfs://10.2.143.5:9090/root/myfile//my2.txt");
		Path p2 = new Path("D://7");
		fs.copyToLocalFile(p1, p2);
		fs.close();// 释放资源
		System.out.println("下载文件夹或文件成功.....");

	}

	/**
	 * 在HDFS上创建一个文件夹
	 * 
	 * **/
	public static void createDirectoryOnHDFS() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("hdfs://10.2.143.5:9090/root/myfile");
		fs.mkdirs(p);
		fs.close();// 释放资源
		System.out.println("创建文件夹成功.....");

	}

	/**
	 * 在HDFS上删除一个文件夹
	 * 
	 * **/
	public static void deleteDirectoryOnHDFS() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("hdfs://10.2.143.5:9090/root/myfile");
		fs.deleteOnExit(p);
		fs.close();// 释放资源
		System.out.println("删除文件夹成功.....");

	}

	/**
	 * 在HDFS上创建一个文件
	 * 
	 * **/
	public static void createFileOnHDFS() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("hdfs://10.2.143.5:9090/root/abc.txt");
		fs.createNewFile(p);
		// fs.create(p);
		fs.close();// 释放资源
		System.out.println("创建文件成功.....");

	}

	/**
	 * 在HDFS上删除一个文件
	 * 
	 * **/
	public static void deleteFileOnHDFS() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("hdfs://10.2.143.5:9090/root/abc.txt");
		fs.deleteOnExit(p);
		fs.close();// 释放资源
		System.out.println("删除成功.....");

	}

	

}
