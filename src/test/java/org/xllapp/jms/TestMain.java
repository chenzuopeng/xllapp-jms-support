package org.xllapp.jms;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *
 *
 * @Copyright: Copyright (c) 2013 FFCS All Rights Reserved 
 * @Company: 北京福富软件有限公司 
 * @author 陈作朋 Sep 15, 2013
 * @version 1.00.00
 * @history:
 * 
 */
public class TestMain {

	public static void main(String[] args) throws JMSException, InterruptedException {
//		while(true){
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("system", "manager","failover:(nio://192.168.56.101:60000)?jms.useAsyncSend=true&randomize=false&timeout=3000&trackMessages=true&priorityBackup=true&priorityURIs=nio://192.168.56.101:60000");
		Connection connection = null;
		Session session = null;
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue("aaa");
			MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			Message message = session.createTextMessage("aaaaaa");
			producer.send(message);
//			Thread.sleep(9999999);
		} finally {
			try {
				if (session != null) {
					session.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
//		}
		Thread.sleep(9999999);
	}
	
}
