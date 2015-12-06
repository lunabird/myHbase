package com.aurora.hbase.mapreduce.preprocess;


/**
 * 重组、预处理运行线程
 */


import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.aurora.hbase.mapreduce.bmp.GenerateBMP;



public class PreProcess
{
	
	
	static String date;
	static String time;
	static String hour;
	static String waveband;
	static String mode;
	
	public static int noiseR=1137,noiseG=546,noiseV=594,LimRayleighR=4000,LimRayleighG=4000,LimRayleighV=4000,
			Rx0=256,Ry0=257,Gx0=261,Gy0=257,Vx0=257,Vy0=255;
    public static float RK= (float) 0.5159,GK=(float) 1.0909,VK=(float) 1.5280,rotatedegree=(float) -61.1;
    //private static File[] array;
    
   
   
//    private static void tree(File f, int level) {       //�ݹ��ȡĿ¼�е������ļ���
//    	  
//    	  String preStr = "";
//    	  for(int i=0; i<level; i++) {
//    	   preStr += "    ";
//    	  }
//    	  
//    	 array = f.listFiles();
//    	  for(int i=0; i<array.length; i++) {
//    	   System.out.println(preStr + array[i].getName()); //�ļ������array������
//    	  if(array[i].isDirectory()) {
//    	    tree(array[i], level + 1);
//    	   }
//    	  }
//    }
    	 
    /**
	 * 从极光原始img图像数据中获取时间信息
	 * 
	 * @param filename
	 * @return
	 */
	public static String readTimeInfo(File file) {
		try {
			FileReader fr = new FileReader(file); // 源文件
			BufferedReader br = new BufferedReader(fr);
			String read = br.readLine();
			// 判断是否采用了Comment工作模式
			br.skip(3 * read.length()); // 跳过3行，读第5行
			String read1 = br.readLine();
			// System.out.println(read1);
			int index_comment = read1.indexOf("[Comment]");

			int index = read.indexOf("Date"); // 获取时间、日期等信息
			date = read.substring(index + 6, index + 10)
					+ read.substring(index + 11, index + 13)
					+ read.substring(index + 14, index + 16);
			if (index_comment == -1) {
				int index1 = read.indexOf("Time");
				time = read.substring(index1 + 6, index1 + 8)
						+ read.substring(index1 + 9, index1 + 11)
						+ read.substring(index1 + 12, index1 + 14);
				hour = read.substring(index1 + 6, index1 + 8);
			} else {
				time = read1.substring(index_comment + 34, index_comment + 36)
						+ read1.substring(index_comment + 37,
								index_comment + 39)
						+ read1.substring(index_comment + 40,
								index_comment + 42);
				hour = read1.substring(index_comment + 34, index_comment + 36);
			}
			fr.close();
			br.close();
		}

		catch (IOException ie) {
			System.err.println(ie.getMessage());
		}
		return date +hour+ time;

	}	
   
	
	public void preprocess(File file) {
		float I, K=0.0f;
		String fileName=file.getName();
		int noise=0,LimRayleigh=1,x0=0,y0=0;
		int r=220;
		waveband = fileName.substring(7, 8); // 重组，查找图片的波段
		readTimeInfo(file);
		
		
		BufferedImage bi = new BufferedImage(512, 512,
				BufferedImage.TYPE_INT_RGB); // 在内存中生成512x512的图像缓冲区，TYPE_INT_RGB表示一个图像，该图像具有整数像素的
												// 8 位 RGB 颜色

		Graphics g = bi.getGraphics();

		//fileName = array[k].getName(); // 获取文件名

		if (fileName.charAt(0) == 'E' && fileName.charAt(1) == 'N') {
			if (fileName.charAt(8) == 'R') { // 判断文件属于哪一个波段

				noise = noiseR;
				LimRayleigh = LimRayleighR;
				x0 = Rx0;
				y0 = Ry0;
				K = RK;
			} else if (fileName.charAt(8) == 'G') {
				noise = noiseG;
				LimRayleigh = LimRayleighG;
				x0 = Gx0;
				y0 = Gy0;
				K = GK;
			} else if (fileName.charAt(8) == 'V') {
				noise = noiseV;
				LimRayleigh = LimRayleighV;
				x0 = Vx0;
				y0 = Vy0;
				K = VK;
			}
		}

		else {
			if (fileName.charAt(7) == 'R') { // 判断文件属于哪一个波段

				noise = noiseR;
				LimRayleigh = LimRayleighR;
				x0 = Rx0;
				y0 = Ry0;
				K = RK;
			} else if (fileName.charAt(7) == 'G') {
				noise = noiseG;
				LimRayleigh = LimRayleighG;
				x0 = Gx0;
				y0 = Gy0;
				K = GK;
			} else if (fileName.charAt(7) == 'V') {
				noise = noiseV;
				LimRayleigh = LimRayleighV;
				x0 = Vx0;
				y0 = Vy0;
				K = VK;
			}
		}
		
					
		
	}
	        
		
	
	
	

	public static void main(String[] args) throws IOException{  //主函数运行程序入口
//		File f = new File("d:/auroramatlab/N20061227G");
//		System.out.println(f.getName());
//		tree(f, 1);
//		processrun rs = new processrun();
//
//		rs.run();
//		System.out.println("done");
		/* 
		 * 把bmp图片直接显示出来
		 * final String fileName ="d:/auroramatlab/N20061207/03/G/N20061207G030220.bmp";  //把这个改成你自己的bmp图片的路径
	        
	        SwingUtilities.invokeLater(new Runnable(){
	            public void run(){
	                new ShowBmp(fileName).setVisible(true);
	            }
	        });*/
        
	}
}


