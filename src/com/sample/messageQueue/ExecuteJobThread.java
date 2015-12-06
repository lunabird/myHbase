/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-21
 */
package com.sample.messageQueue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;


/**
 * @author hadoop
 *
 */

class ExecuteJobThread extends Thread {
	static Queue<String> queue = Server.queue;
	public void run(){
		while(queue.size()>0){
			synchronized (queue) {
				String result = queue.poll();
				System.out.println("consume result "+result);
			}
		}
	}
	public boolean executeVMScript(String strCmdPath) {
		try {
			Process process = Runtime.getRuntime().exec(
					"chmod 777 " + strCmdPath);// 修改文件权限成可执行的
			process = Runtime.getRuntime().exec(strCmdPath);
			BufferedReader strCon = new BufferedReader(new InputStreamReader(
					process.getInputStream())); // 在控制台输出结果
			String line;
			 
			 while ((line = strCon.readLine()) != null) {
			 }
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
}
