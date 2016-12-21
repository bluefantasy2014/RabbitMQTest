package rabbit.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitManager {
	private static final Logger LOG = Logger.getLogger(RabbitManager.class);
	
	private String URL; 
	private Connection conn; 
	
	public RabbitManager(String url){
		this.URL = url; 
		init(); 
	}
	
	private void init(){
		ConnectionFactory cf = new ConnectionFactory(); 
		cf.setAutomaticRecoveryEnabled(true);
		cf.setNetworkRecoveryInterval(1000);
		
		try {
			cf.setUri(URL);
			conn = cf.newConnection(); 
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void tearDown(){
		if (this.conn != null){
			try {
				this.conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void pubMsg(String exchange,String msg){
		Channel channel = null; 
		
		try {
			channel = conn.createChannel();
			channel.exchangeDeclare(exchange, "fanout",true); 
			channel.basicPublish(exchange, "", null, msg.getBytes());
			LOG.info("sending msg:" + msg + " to "+ exchange);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (channel != null){
				try {
					channel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
