package com.aurora.hbase.mapreduce.bmp;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.WritableRaster;
import java.awt.*;
import java.awt.geom.AffineTransform;

import java.io.File;
/**
 * 图像缩放类，形成缩略图
 */
public class Miniature {
  
	/**
	 * 生成图像的缩略图
	 * @param source
	 * @param targetW
	 * @param targetH
	 * @return
	 */
	public static BufferedImage resize(BufferedImage source, int targetW, int targetH)  // targetW，targetH分别表示目标长和宽
	{
        int type = source.getType();
        BufferedImage target = null;
        double sx = (double) targetW / source.getWidth();
        double sy = (double) targetH / source.getHeight();
        if(sx>sy)
        {
          sx = sy;
          targetW = (int)(sx * source.getWidth());
        }
        else{
          sy = sx;
          targetH = (int)(sy * source.getHeight());
         }
        if (type == BufferedImage.TYPE_CUSTOM) { //handmade
             ColorModel cm = source.getColorModel();
             WritableRaster raster = cm.createCompatibleWritableRaster(targetW, targetH);
             boolean alphaPremultiplied = cm.isAlphaPremultiplied();
             target = new BufferedImage(cm, raster, alphaPremultiplied, null);
           } 
        else
           target = new BufferedImage(targetW, targetH, type);
           Graphics2D g = target.createGraphics();
       //smoother than exlax:
       g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY );
       g.drawRenderedImage(source, AffineTransform.getScaleInstance(sx, sy));
       g.dispose();
       return target;
    }
    
	
	/**
	 * 生成bmp格式的极光图像
	 * @param fromFileStr
	 * @param saveToFileStr
	 * @param width
	 * @param hight
	 * @throws Exception
	 */
	public static void saveImageAsBmp (String fromFileStr,String saveToFileStr,int width,int hight)
    throws Exception {
         BufferedImage srcImage;
         String imgType = "bmp";
         if (fromFileStr.toLowerCase().endsWith(".bmp")) {
         imgType = "bmp";
         }
         File saveFile=new File(saveToFileStr);
         File fromFile=new File(fromFileStr);
         srcImage = ImageIO.read(fromFile);
         if(width > 0 || hight > 0)
         {
            srcImage = resize(srcImage, width, hight);
         }
         ImageIO.write(srcImage, imgType, saveFile);
     }
	  
}
 