///**
// * @brief 
// * @author huangpeng
// * @version 
// * @date 2015-11-3
// */
//package com.sample.hello.test;
//
//
//
//import de.lmu.ifi.dbs.jfeaturelib.features.Haralick;
//import de.lmu.ifi.dbs.utilities.Arrays2;
//import ij.process.ColorProcessor;
//import java.io.File;
//import java.io.IOException;
//import java.net.URISyntaxException;
//import java.util.List;
//import javax.imageio.ImageIO;
//
///**
// * @author hadoop
// *
// */
//public class JFeatureTest {
//	
//	 public static void main(String[] args) throws IOException, URISyntaxException {
//	        // load the image
//	        File f = new File(JFeatureTest.class.getResource("/test.jpg").toURI());
//	        ColorProcessor image = new ColorProcessor(ImageIO.read(f));
//
//	        // initialize the descriptor
//	        Haralick descriptor = new Haralick();
//
//	        // run the descriptor and extract the features
//	        descriptor.run(image);
//
//	        // obtain the features
//	        List<double[]> features = descriptor.getFeatures();
//
//	        // print the features to system out
//	        for (double[] feature : features) {
//	            System.out.println(Arrays2.join(feature, ", ", "%.5f"));
//	        }
//	    }
//
//}
