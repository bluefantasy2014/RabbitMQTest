package rabbit.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RabbitMsgReceiver {
	private static String url = "amqp://test:123456@192.168.0.100:5672/peking_integration" ; 
	private static String EXCHANAGE_NAME = "exchange_test_fanout"; 
	private static String QUEUE_NAME = "queue_test_0"; 
	 private static final Logger LOG = Logger.getLogger(RabbitMsgReceiver.class);
	
	private String URL ; 
	private ConnectionFactory cf; 
	private Connection conn;
	private boolean autoAck = false; 
	private boolean preFetch = true; 
	
	public RabbitMsgReceiver(String url){
		this.URL = url; 
		this.cf = new ConnectionFactory(); 
		try {
			this.cf.setUri(URL);
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		this.cf.setAutomaticRecoveryEnabled(true);
	    this.cf.setNetworkRecoveryInterval(1000);
	}
	
	public void recvMsg(){
		Channel channel = null;
		
		try {
			conn = cf.newConnection(); 
			channel = conn.createChannel(); 
			channel.exchangeDeclare(EXCHANAGE_NAME, "fanout",true); 
			channel.queueDeclare(QUEUE_NAME, true, false, false, null); 
			channel.queueBind(QUEUE_NAME, EXCHANAGE_NAME, ""); 
			
			if(preFetch){
				channel.basicQos(10);
			}
			
			QueueingConsumer consumer = new QueueingConsumer(channel); 
			channel.basicConsume(QUEUE_NAME, autoAck,consumer); 
			
			LOG.info("Start receiving messages");
			
			while (true){
				QueueingConsumer.Delivery delivery = consumer.nextDelivery(); 
				String msgBody = new String(delivery.getBody()); 
				LOG.info("recved msg: " + msgBody);
				
				LOG.info("Sleep to simulate the work need to do");
				Thread.sleep(4000);
				if (!autoAck){
					long tag = delivery.getEnvelope().getDeliveryTag(); 
					channel.basicAck(tag, false);
					LOG.info("manual acked msg: " + msgBody + " with deliveryTag:" + tag);
				}
			}
		}catch (Throwable t){
			t.printStackTrace();
		}finally {
			if (channel!=null){
				try {
					channel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (conn != null){
				try {
					conn.close();
				} catch (IOException e) {
					e.printStackTrace();
				} 
			}
		}
	}
	
	
	public static void main(String[] args){
		PropertyConfigurator.configure("log4j.properties");
		new RabbitMsgReceiver(url).recvMsg();
	}
}
