package com.aurora.hbase.mapreduce.keogram;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.BufferedImage;
import java.awt.image.MemoryImageSource;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import javax.swing.JPanel;


public class ChooseKeodata{

	public static int[] chooseData(File file) {

		int R = 220, w = 10, h = 256;
		String fileName, date, time, band;
		int keo_data[] = new int[440];
		int keoB[] = new int[2 * R];
		int[] srcPixArray = new int[w * h];

		fileName = file.getName();
		date = fileName.substring(0, 9);
		time = "20";
		band = fileName.substring(9, 10);

		File rs = file;
		System.out.println("filename:" + fileName);
		System.out.println("rs:" + rs);
		BufferedImage img = null;
		try {
			img = ImageIO.read(rs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int j = 0; j < 2 * R; j++) {
			keoB[j] = img.getRGB(221, j) & 0xff; // 获取某个点的颜色ֵ
			keo_data[j] = keoB[j];
		}
		return keo_data;
	}
/*
		BufferedImage bi = new BufferedImage(filelist.length, 440,
				BufferedImage.TYPE_INT_RGB);
		Graphics2D g2 = (Graphics2D) bi.getGraphics();
		g2.setColor(Color.white);
		g2.fillRect(0, 0, filelist.length, 440);

		int x = 221;
//		for (int s = 0; s < filelist.length; s++) {
			for (int y = 0; y < 2 * R; y++) {
				// 获取到rgb的组合值ֵ
				int rgb = keo_data[y][s];
				Color color = new Color(rgb);
				int r = color.getRed();
				int g = color.getGreen();
				int b = color.getBlue();
				// System.out.println(rgb);

				if (rgb <= 51) {
					r = 0;
					g = rgb * 5;
					b = 255;
				} else if (51 < rgb && rgb <= 102) {
					r = 0;
					g = 255;
					b = 255 - (rgb - 51) * 5;
				} else if (102 < rgb && rgb <= 153) {
					r = (rgb - 102) * 5;
					g = 255;
					b = 0;
				} else if (153 < rgb && rgb <= 204) {
					r = 255;
					g = (int) Math
							.round(255 - (128.0 * (rgb - 153) / 51.0 + 0.5));
					b = 0;
				} else if (204 < rgb && rgb <= 255) {
					r = 255;
					g = (int) Math
							.round(127 - (127.0 * (rgb - 204) / 51.0 + 0.5));
					b = 0;
				}

				Color c = new Color(r, g, b);
				// bi.setRGB(x, y, color.getRGB());
				// System.out.println(r);
				g2.setColor(c); // 此处为选则画点的颜色，R=G=B=0为黑色。R=G=B=1为白色
				g2.drawLine(s, y, s, y);

			}
//		}
		//结果输出到一个bmp文件中
		File file1 = new File(destKeogramGraphPath);
		try {
			ImageIO.write(bi, "bmp", file1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("keogram done successfully");

	}
*/
}
