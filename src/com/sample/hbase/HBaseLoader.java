package com.sample.hbase;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Loads files into a table:
 * 	* Tablename: 'files'
 * 	* ColumnFamiy: 'content:'
 * 
 * Use:
 * 	HBaseLoader <tableName> <dir>
 * 
 * @params tableName Name of the table where to import files
 * @params dir Directory containing files to import
 * 
 * Diego Pino García <dpino@igalia.com>
 *
 */
public class HBaseLoader {

	private static final HBaseHelper hbase =  HBaseHelper.create();
	
    public static void main(String[] args) {    	
//    	args[0] = "E:\\testTmp\\pic\\";
//    	args[1] = "hpPicTable";
//    	if (args.length < 1 || args.length > 2) {
//    		error("HBaseLoader <tablename> <dir>");
//    		return;
//    	}
    	
		if (hbase == null) {
			error("Couldn't establish connection to HBase");
		}

		String fileName = "E:\\AuroraRawData-2004\\200411\\N20041109G_F\\";//args[1];
		File file = new File(fileName);
		if (file == null) {
			error(String.format("Directory %s doesn't exist", fileName));
		}
		if (!file.isDirectory()) {
			error(String.format("File %s is not a directory", fileName));
		}
		
		HTableInterface table = null;
		try {
			String tableName = "RawImgTable";//args[0];
			/**
			 * 要改变表结构时重新运行该句
			 */
			hbase.dropTable("hpPicTable");
			table = hbase.getOrCreateTable(tableName);
			for (File each: file.listFiles()) {
				if (each.isFile()) {
					insertInto(table, each);
				}
			}
			System.out.println("insert into table end successfully!");
			//关闭htable
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
    }

	private static void insertInto(HTableInterface table, File file) {
		try {
//			byte[] content = Bytes.toBytes(FileUtils.readFileToString(file, "UTF-8"));
//			byte[] content = image2Bytes(file.getAbsolutePath());
			byte[] content = FileUtils.readFileToByteArray(file);//将图片内容转换成字节流
			hbase.insert(table, file.getName(),content);
			System.out.println("insert data into "+new String(table.getTableName())+" ok!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*public static byte[] image2Bytes(String imagePath) {
		ImageIcon ima = new ImageIcon(imagePath);
		BufferedImage bu = new BufferedImage(ima.getImage().getWidth(null), ima
				.getImage().getHeight(null), BufferedImage.TYPE_INT_RGB);
		ByteArrayOutputStream imageStream = new ByteArrayOutputStream();
		try {
			// 把这个jpg图像写到这个流中去,这里可以转变图片的编码格式
			boolean resultWrite = ImageIO.write(bu, "jpg", imageStream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] tagInfo = imageStream.toByteArray();
		return tagInfo;
	}*/
	private static void error(String str) {
		System.out.println("Error: " + str);
	}
    
}
