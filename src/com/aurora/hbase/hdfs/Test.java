/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-3
 */
package com.aurora.hbase.hdfs;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.apache.hadoop.hbase.KeyValue;

import com.aroura.hbase.util.DataRead;
import com.aroura.hbase.util.HBaseJavaAPI;
import com.aurora.hbase.mapreduce.bmp.GenerateBMP;

/**
 * @author hadoop
 *
 */
public class Test {
	public static void main(String[] args) throws Exception{
		
		
		HBaseJavaAPI.getAllRows("HAuroraImgTable");//获取表中的所有记录
		
		/*//下载图片
		KeyValue kv = HBaseJavaAPI.getDataByRowKeyWithoutQualifier("HAuroraImgTable", "N0104110900001G","data");
		byte[] imgRaw = kv.getValue();
		
		File imgFile = DataRead.byteToFile(imgRaw);
		*/
		
		
//		File fromFile=imgFile;
//		
//		
//		BufferedImage source = ImageIO.read(fromFile);
//		BufferedImage image = GenerateBMP.convertImgToBmp(fromFile);
	}
	
	
	public void test1(){
		ArrayList<String> list = CopyToHDFS.readLogTxt();
		String sourcePath = list.get(0);
		String[] tmp = sourcePath.split("\\\\");
		String fileName = "E:/AuroraRawData-2004/200411/N20041109G_F/"+tmp[tmp.length-1];
		System.out.println(fileName);
		String myrowkey = tmp[tmp.length-1].split("\\.")[0];
		System.out.println("rowkey:"+myrowkey);
		
		char[] rk = myrowkey.toCharArray();
		String rowkey = rk[0]+"01"+rk[1]+rk[2]+rk[3]+rk[4]+rk[5]+rk[6]+rk[8]+rk[9]+rk[10]+rk[11]+rk[12]+rk[7];
		System.out.println("rowkey:"+rowkey);
		
		long start = System.currentTimeMillis();
		String newrow = null;
		for(int i=0;i<10000000;i++){
			newrow = rowkey.substring(0, 2)+"2"+rowkey.substring(3);
//			System.out.println("newrow:"+newrow);
		}
		long end = System.currentTimeMillis();
		long spendtime = end-start;
		System.out.println("time1:"+spendtime);
		
		
		start = System.currentTimeMillis();
		for(int i=0;i<10000000;i++){
			char[] q = rowkey.toCharArray();
			q[2] = '2';
			q.toString();
			//System.out.println("newrow:"+);
		}		
		end = System.currentTimeMillis();
		spendtime = end-start;
		System.out.println("time2:"+spendtime);
	}
}
