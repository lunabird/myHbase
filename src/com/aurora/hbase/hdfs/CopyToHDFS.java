package com.aurora.hbase.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;

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
	//本地文件夹里的文件列表
	public static File[] array;
	
	public static void main(String[] args) throws Exception {
		
		
		
		
		//CopyToHDFS.myTree("E:\\AuroraRawData-2004\\200411\\N20041112G_F",1);//获取所有该目录下的图像文件名
		
		//拷贝图像数据到HDFS
		/*ArrayList<String> list = CopyToHDFS.readLogTxt();
		for(int i=0;i<list.size();i++){
			String sourcePath = list.get(i);
//			System.out.println(sourcePath);
			String[] tmp = sourcePath.split("\\\\");
//			System.out.println(tmp[tmp.length-1]);
			String targetPath = "hdfs://192.168.0.120:9000/user/hadoop/huangpeng/picture/"+tmp[tmp.length-1];
//			CopyToHDFS.copyFilesFromWinToHDFS(sourcePath,targetPath);
		}*/
		
//		hdfsDeleteFile();
		
		//拷贝wordcount例子到HDFS
		//CopyToHDFS.copyFilesFromWinToHDFS("Data\\wordCountInputFile.txt","hdfs://192.168.0.120:9000/user/hadoop/huangpeng/mr/");
		
		CopyToHDFS.copyFilesFromWinToHDFS("F:\\tmp\\mr001.jar","hdfs://192.168.0.120:9000/user/hadoop/huangpeng/mr/mr001.jar");
	}
	
	public static void copyFilesFromWinToHDFS(String sourcePath,String targetPath) throws IOException{
		 //将本地文件上传到hdfs。
//		  String target="hdfs://192.168.0.120:9000/user/hadoop/huangpeng/picture/N041111G00002.img";
		String target=targetPath;
//		  FileInputStream fis=new FileInputStream(new File("D:\\auroramatlab\\N20061227G\\N041111G00002.img"));//读取本地文件
		  FileInputStream fis=new FileInputStream(new File(sourcePath));
		  Configuration config=new Configuration();
		  FileSystem fs=FileSystem.get(URI.create(target), config);
		  OutputStream os=fs.create(new Path(target));
		  //copy
		  IOUtils.copyBytes(fis, os, 526336, true);
		  System.out.println(target+"拷贝完成...");
		
	}
	
	/**
	 * hdfs删除文件
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void hdfsDeleteFile(String path) throws IOException {
        Configuration conf=new Configuration();
        FileSystem hdfs=FileSystem.get(conf);
       
        Path delef=new Path("file:///192.168.0.120:9000/user/hadoop/huangpeng/picture/N041111G00002.img");
       
        boolean isDeleted=hdfs.delete(delef,false);
        //递归删除
        //boolean isDeleted=hdfs.delete(delef,true);
        System.out.println("file Delete?"+isDeleted);
    }
	
	/**
	 * 递归获取目录中的所有文件名
	 * @param f 文件夹名字
	 * @param level 开始为1
	 */
	public static void myTree(String path,int level) {
		
		String preStr = "";
		for (int i = 0; i < level; i++) {
			preStr += "    ";
		}
		
		File file = new File(path);
		File[] tempList = file.listFiles();
		//writeToLogTxt("该目录下对象个数：" + tempList.length);
		for (int i = 0; i < tempList.length; i++) {
			if (tempList[i].isFile()) {
//				writeToLogTxt(preStr + "文     件：" + tempList[i]);
				writeToLogTxt(tempList[i]+"");
			}
			if (tempList[i].isDirectory()) {
				//writeToLogTxt(preStr + "文件夹：" + tempList[i]);
				myTree(tempList[i].getAbsolutePath(),level+1);
			}
		}
	}
	/**
	 * 将结果写到txt文件里	
	 * @param a
	 */
	public static void writeToLogTxt(String a) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
					"Data\\N20041112G_F.txt"),true));
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
	/**
	 * 获取所有的文件名
	 * @return
	 */
	public static ArrayList<String> readLogTxt(){
		ArrayList<String> list = new  ArrayList<String>();
		String fileName = "Data/log.txt";
        BufferedReader fromFile = null;
        try {
            FileReader fr = new FileReader(fileName);
            fromFile = new BufferedReader(fr);
//            System.out.println("The file " + fileName+ " contains the following lines:");
            String line = fromFile.readLine();
            while (line != null) {
            	list.add(line);
//                System.out.println(line);
                line = fromFile.readLine(); // throw IOException
                
            } 
        }catch(IOException e){
        	e.printStackTrace();
        }
//        System.out.println(list.get(77350));
        return list;
	}
	
	
	
}
