/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-21
 */
package com.sample.messageQueue;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;


/**
 * @author hadoop
 *
 */
class Server extends Thread{
	private static final int listenPort = 7001;
	private Socket socket;//接入的客户端Socket
	private ServerSocket serverSocket;
	static Queue<String> queue = new LinkedList<String>();
	
	@Override
	public void run() {			
		try {
			System.out.println("monListenPort:"+listenPort);
			serverSocket = new ServerSocket(listenPort);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("TCP 开始监听端口"+listenPort+"...");
		while (true) {
			try {
				socket = serverSocket.accept();
				System.out.println("有客户端/其他节点接入: "
						+ socket.getInetAddress().getHostAddress());
				ObjectInputStream ois;
				ois = new ObjectInputStream(socket.getInputStream());
				String msg =  (String)ois.readObject();
				queue.add(msg);
				System.out.println(queue.size());
				new ExecuteJobThread().start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			
		}
	}
	public static void main(String[] args){
		Server s = new Server();
		s.start();
	}
}

