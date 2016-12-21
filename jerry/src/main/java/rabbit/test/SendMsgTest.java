package rabbit.test;

import org.apache.log4j.PropertyConfigurator;

import rabbit.utils.RabbitManager;

public class SendMsgTest {
	private static String url = "amqp://test:123456@192.168.0.100:5672/peking_integration" ; 
	private static String EXCHANAGE_NAME = "exchange_test_fanout"; 
	private static String QUEUE_NAME = "queue_test_0"; 
	
	public static void main(String [ ] args){
		PropertyConfigurator.configure("log4j.properties");
		RabbitManager mgr = new RabbitManager(url); 
		for (int i=0; i<100; ++i){
			mgr.pubMsg(EXCHANAGE_NAME, "testMsg_" +i);
		}
		
		mgr.tearDown();
		int a = 100; 
	}
}
