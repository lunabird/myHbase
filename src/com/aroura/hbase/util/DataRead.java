package com.aroura.hbase.util;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;


/**
 * 读取原IMG格式的图像数据
 */
public class DataRead {
	public static int[][] GetData(String FileName) throws Exception {
		RandomAccessFile rFile = new RandomAccessFile(FileName, "r");   //随机读取流
		
		int[][] result = new int[512][512];
		long point = rFile.length() - 524288; //524288=512*512*2，设置指针距文件结尾512*512*2字节处，然后读取一个512x512的矩阵
		rFile.seek(point);   // file.seek(int类型的参数)表示从文件的第几个位置开始搜索
	    for(int i=0;i<512;i++)
	    	for(int j=0;j<512;j++)
	    	{
	    		result[i][j]=getByte(rFile.readByte(),rFile.readByte());  //获取字节信息
	    		//System.out.println(result[i][j]);
	    	}
	
		rFile.close();//关闭流
		return result;
		
	}
	
	public static int getByte(byte a,byte b){
		return (int) ((b& 0xFF)*256+ (a & 0xFF));	 //byte型转换成int型
		
	}
	
	/**
	 * Description:把一个byte数组类型的文件还原为File Author: Time: 2015-10-08 下午19:26:46
	 * 
	 * @param file
	 *            需要转换的byte数组
	 * @param fileName
	 *            转换的文件名
	 * @return 转换成功以后返回转换后的文件，否则返回null
	 */
	public static File byteToFile(byte[] file) {
		File result = null;
		BufferedOutputStream outStream = null;
		try {
			//result = new File(fileName);
			result = new File("Data\\tempImgFile.img");
			OutputStream tmpStream = new FileOutputStream(result);
			outStream = new BufferedOutputStream(tmpStream);
			outStream.write(file);
			System.out.println("file write to Data\\tempImgFile.img successfully!");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (outStream != null)
					outStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	
}
