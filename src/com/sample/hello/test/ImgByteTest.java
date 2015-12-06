/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-8
 */
package com.sample.hello.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

/**
 * @author hadoop
 *
 */
public class ImgByteTest {
	
	/**
	 * Description:把一个byte数组类型的文件还原为File
	 * Author: 
	 * Time: 2015-10-08 下午19:26:46
	 * @param file  需要转换的byte数组
	 * @param fileName  转换的文件名
	 * @return 转换成功以后返回转换后的文件，否则返回null
	 */
	public static File byteToFile(byte[] file,String fileName){
		File result = null;
		BufferedOutputStream outStream = null;
		try {
			result = new File(fileName);
			OutputStream tmpStream = new FileOutputStream(result);
			outStream = new BufferedOutputStream(tmpStream);
			outStream.write(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(outStream != null)
					outStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public static void main(String[] args) throws IOException{
		String fileName = "D:\\auroramatlab\\N20061227G\\N041111G00001.img";
		File file = new File(fileName);
		byte[] content = FileUtils.readFileToByteArray(file);//将图片内容转换成字节流
		//System.out.println(Arrays.toString(content));
		
		byteToFile(content,"D:\\auroramatlab\\test.img");
	}

}
