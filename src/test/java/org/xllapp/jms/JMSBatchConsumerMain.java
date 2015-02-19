package org.xllapp.jms;

import javax.jms.Message;
import javax.jms.MessageListener;

import org.xllapp.jms.BatchJMSConsumer;

/**
 *
 *
 * @Copyright: Copyright (c) 2013 FFCS All Rights Reserved 
 * @Company: 北京福富软件有限公司 
 * @author 陈作朋 Oct 6, 2013
 * @version 1.00.00
 * @history:
 * 
 */
public class JMSBatchConsumerMain {

	public static void main(String[] args) throws Exception {
		String brokerUrl="tcp://localhost:60000";
		String userName="system";
		String password="manager";
		String queue="test";
		BatchJMSConsumer jmsBatchConsumer=new BatchJMSConsumer(brokerUrl, userName, password, queue,10);
		
/*		for (int i = 0; i < 10000; i++) {
			for (Message message : jmsBatchConsumer.getMessages()) {
				System.out.println(i+":"+MessageHelper.format(message, message.toString()));
			}
			Thread.sleep(5000);
		}*/
		
		jmsBatchConsumer.setMessageListener(new MessageListener() {
			
			@Override
			public void onMessage(Message message) {
//				System.out.println(MessageHelper.toString(message));
/*				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}*/
			}
		});
		
		jmsBatchConsumer.start();

	}

}
