package com.sample.git.hbase;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.swing.ImageIcon;

public class imageTest {

	/*public static void main(String[] args) throws IOException{
		byte[] a = image2Bytes("E:\\testTmp\\pic\\download.jpg");
		System.out.println(checkImageType(a));
	}*/
	public static void main(String[] args) {
		 
		try {
 
			byte[] imageInByte;
			BufferedImage originalImage = ImageIO.read(new File(
					"E:\\testTmp\\pic\\aasas.bmp"));
 
			// convert BufferedImage to byte array
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(originalImage, "jpg", baos);
			baos.flush();
			imageInByte = baos.toByteArray();
			baos.close();
 
			// convert byte array back to BufferedImage
			InputStream in = new ByteArrayInputStream(imageInByte);
			BufferedImage bImageFromConvert = ImageIO.read(in);
 
			ImageIO.write(bImageFromConvert, "jpg", new File(
					"E:\\aasas.bmp"));
 
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
	
	// 将图片转成字节流
	public static byte[] image2Bytes(String imagePath) {
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
	}

	// 根据字节流来判断这个图片的编码格式

	public static String checkImageType(byte[] imageBytes) throws IOException {
		ByteArrayInputStream bais = null;
		MemoryCacheImageInputStream mcis = null;
		try {
			bais = new ByteArrayInputStream(imageBytes);
			mcis = new MemoryCacheImageInputStream(bais);
			Iterator<ImageReader> itr = ImageIO.getImageReaders(mcis);
			while (itr.hasNext()) {
				ImageReader reader = (ImageReader) itr.next();
				String imageName = reader.getClass().getSimpleName();
				if (imageName != null
						&& ("JPEGImageReader".equalsIgnoreCase(imageName))) {
					return "jpeg";
				} else if (imageName != null
						&& ("JPGImageReader".equalsIgnoreCase(imageName))) {
					return "jpg";
				} else if (imageName != null
						&& ("pngImageReader".equalsIgnoreCase(imageName))) {
					return "png";
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		Iterator writers = ImageIO.getImageWritersByFormatName("jpg");
		ImageWriter writer =(ImageWriter)writers.next();
		File f = new File("E:\\dd.jpg");
		ImageOutputStream ios = ImageIO.createImageOutputStream(f);
		writer.setOutput(ios);
		BufferedImage bi = null;
		writer.write(bi);
		return null;
	}
	

}
