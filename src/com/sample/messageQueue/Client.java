/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-21
 */
package com.sample.messageQueue;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author hadoop
 *
 */
public class Client {

	public static void sendMessage(String a) throws IOException{
		Socket s = new Socket("localhost", 7001);
		// 发送Socket消息
		OutputStream ops = s.getOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(ops);
		String aMsg = a;
		oos.writeObject(aMsg);
	}
	
	public static void main(String[] args) throws IOException{
//		for(int i=0;i<10;i++){
//			sendMessage(i+"");
//		}
		
		
		
		
	}
	
}
