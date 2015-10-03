package com.sample.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * @brief 拷贝文件到HDFS
 * @author huangpeng
 * @date 2015/3/11
 */
public class CopyToHDFS {
	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path source = new Path("/home/hadoop/word.txt");
//		Path dst = new Path("/user/root/njupt/");
//		fs.copyFromLocalFile(source, dst);
		CopyToHDFS.copyFilesFromWinToHDFS();
		
		
	}
	
	public static void copyFilesFromWinToHDFS() throws IOException{
		 //将本地文件上传到hdfs。
		  String target="hdfs://192.168.0.120:9000/user/hadoop/huangpeng/picture/N041111G00002.img";
		  FileInputStream fis=new FileInputStream(new File("D:\\auroramatlab\\N20061227G\\N041111G00002.img"));//读取本地文件
		  Configuration config=new Configuration();
		  FileSystem fs=FileSystem.get(URI.create(target), config);
		  OutputStream os=fs.create(new Path(target));
		  //copy
		  IOUtils.copyBytes(fis, os, 526336, true);
		  System.out.println("拷贝完成...");
		
		 /*String[] str = new String[]{"D:\\auroramatlab\\info.txt"};
	        Configuration conf = new Configuration();
	        
	        Path dstDir = new Path("hdfs:///192.168.0.120:9000/user/hadoop/huangpeng/picture/info.txt");  
	        FileSystem hdfs =  FileSystem.get(conf);  
	        
//	        FileSystem fileS= FileSystem.get(conf);
	        FileSystem localFile = FileSystem.getLocal(conf);  //得到一个本地的FileSystem对象
	         
	        Path input = new Path(str[0]); //设定文件输入保存路径
	        Path out = dstDir;  //文件到hdfs输出路径
	         
	        try{
	            FileStatus[] inputFile = localFile.listStatus(input);  //listStatus得到输入文件路径的文件列表
	            FSDataOutputStream outStream = hdfs.create(out);      //创建输出流    
	            for (int i = 0; i < inputFile.length; i++) {
	                System.out.println(inputFile[i].getPath().getName());
	                FSDataInputStream in = localFile.open(inputFile[i].getPath());
	                 
	                byte buffer[] = new byte[1024];
	                int bytesRead = 0;
	                while((bytesRead = in.read(buffer))>0){  //按照字节读取数据
	                    System.out.println(buffer);
	                    outStream.write(buffer,0,bytesRead);
	                }
	                 
	                in.close();
	            }
	            System.out.println("拷贝完成...");
	        }catch(Exception e){
	            e.printStackTrace();
	        }*/
	}
}
