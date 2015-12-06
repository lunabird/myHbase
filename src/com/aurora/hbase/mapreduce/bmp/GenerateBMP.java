package com.aurora.hbase.mapreduce.bmp;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import com.aroura.hbase.util.DataRead;

/**
 * 解析原始图像格式,将图像转化为bmp格式
 * 
 */
public class GenerateBMP {
	
	public static int noiseR=1137,noiseG=546,noiseV=594,LimRayleighR=4000,LimRayleighG=4000,LimRayleighV=4000,
			Rx0=256,Ry0=257,Gx0=261,Gy0=257,Vx0=257,Vy0=255;
    public static float RK= (float) 0.5159,GK=(float) 1.0909,VK=(float) 1.5280;
    public static float	rotatedegree=(float) -61.1;

	

	

	/**
	 * 将IMG类型的极光图片转换为bmp类型
	 * @param fileName
	 * @return
	 */
	public static BufferedImage convertImgToBmp(File file) {
		float I, K = 0.0f;
		String fileName = file.getName();
		int noise = 0, LimRayleigh = 1, x0 = 0, y0 = 0;
		int r = 220;

		BufferedImage bi = new BufferedImage(512, 512,
				BufferedImage.TYPE_INT_RGB); // 在内存中生成512x512的图像缓冲区，TYPE_INT_RGB表示一个图像，该图像具有整数像素的
												// 8 位 RGB 颜色

		Graphics g = bi.getGraphics();

		try {
			int result[][] = DataRead.GetData(fileName); // 读入img格式文件数据

			for (int i = 0; i <= 511; i++) {
				for (int j = 0; j <= 511; j++) {
					int p = result[j][i]; // 进行读出文件字节的转秩
					I = (p - noise) * K;
					I = (I / LimRayleigh);

					if (Math.pow((j - y0), 2) + Math.pow((i - x0), 2) >= Math
							.pow(r, 2)) // 半径r圆形区域外的区域填充黑色
						I = 0;
					else {
						if (I < 0)
							I = 0;
						if (I > 1)
							I = 1;
					}

					g.setColor(new Color(I, I, I)); // 此处为选则画点的颜色，R=G=B=0为黑色。R=G=B=1为白色
					g.drawLine(i, j, i, j); // 在（i,j）处以上面的颜色画点

				}

			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		BufferedImage img = new BufferedImage(512, 512,
				BufferedImage.TYPE_INT_RGB); // 对图像进行-61.1度的逆时针旋转
		BufferedImage img1 = new BufferedImage(440, 440,
				BufferedImage.TYPE_INT_RGB);
		AffineTransform transform = new AffineTransform();
		// transform.translate(x0, y0);
		transform.rotate(rotatedegree * Math.PI / 180, x0, y0);
		// transform.translate(-x0, -y0);

//		AffineTransformOp op = new AffineTransformOp(transform, null);
//		op.filter(bi, img);
//		img1 = img.getSubimage(x0 - r, y0 - r, 440, 440);

		bi.flush();
		img.flush();

		
		return img1;
	}

	/**
	 * 创建文件夹，即新路径
	 * 
	 * @param path
	 */

//	public static void createDir(String path) {
//		File dir = new File(path);
//		if (!dir.exists())
//			dir.mkdir();
//	}
//
//	public static void StringPreBuffer() throws IOException // 生成info文档
//	{
//		String name = "N" + date + waveband + time + ".bmp";
//		File file = new File("d:\\auroramatlab" + "\\info.txt");
//		if (!file.exists())
//			file.createNewFile();
//		FileOutputStream out = new FileOutputStream(file, true);
//		StringBuffer sb = new StringBuffer();
//		sb.append(name + "," + "1" + "," + "d:\\auroramatlab" + "\\\\\\\\"
//				+ "N" + GenerateBMP.date + "\\\\\\\\" + GenerateBMP.hour
//				+ "\\\\\\\\" + GenerateBMP.waveband + "\\\\\\\\" + name + ","
//				+ GenerateBMP.date + "," + GenerateBMP.time + ","
//				+ GenerateBMP.waveband + "," + "d:\\auroramatlab" + "\\\\\\\\"
//				+ "N" + GenerateBMP.date + "\\\\\\\\" + GenerateBMP.hour
//				+ "\\\\\\\\" + GenerateBMP.waveband + "\\\\\\\\" + "mini"
//				+ name + "," + " " + "\r\n");
//
//		out.write(sb.toString().getBytes("utf-8"));
//		out.close();
//	}

}
