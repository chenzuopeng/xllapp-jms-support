package org.xllapp.jms;

import javax.jms.Message;
import javax.jms.MessageListener;

import org.xllapp.jms.JMSConsumer;

/**
 * 
 * 
 * @Copyright: Copyright (c) 2008 FFCS All Rights Reserved
 * @Company: 北京福富软件有限公司
 * @author 陈作朋 Aug 20, 2013
 * @version 1.00.00
 * @history:
 * 
 */
public class JMSConsumerMain {

	public static void main(String[] args) throws Exception {
		JMSConsumer consumer = new JMSConsumer();
		consumer.setBrokerUrl("tcp://localhost:60000");
		// consumer.setBrokerUrl("tcp://192.168.52.36:60000");
		// consumer.setQueue("ST-1033");
		// consumer.setBrokerUrl("failover:(tcp://192.168.0.211:60000)?randomize=false&timeout=3000&trackMessages=true&priorityBackup=true&priorityURIs=tcp://192.168.0.211:60000");
		consumer.setQueue("test");
		consumer.setUserName("system");
		consumer.setPassword("manager");
		consumer.setQueuePrefetch(10);
		consumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				System.out.println(message);
			}
		});
		consumer.start();

		/*
		 * Thread.sleep(5000); consumer.shutdown();
		 */
	}

}
