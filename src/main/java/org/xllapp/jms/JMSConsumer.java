package org.xllapp.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS消费者.
 * 
 * @author dylan.chen Aug 18, 2013
 * 
 */
public class JMSConsumer implements ExceptionListener {

	private final static Logger logger = LoggerFactory.getLogger(JMSConsumer.class);
	
	public final static int DEFAULT_QUEUE_PREFETCH=10;

	private String brokerUrl;
	
	private String userName;

	private String password;

	private String queue;

	private int queuePrefetch=DEFAULT_QUEUE_PREFETCH;

	private MessageListener messageListener;
	
	private Connection connection;
	
	private Session session;

	public void start() throws Exception {
		logger.info("starting JMSConsumer");
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.userName, this.password, this.brokerUrl);
		connection = connectionFactory.createConnection();
		ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
		prefetchPolicy.setQueuePrefetch(queuePrefetch);
		((ActiveMQConnection) connection).setPrefetchPolicy(prefetchPolicy);
		connection.setExceptionListener(this);
		connection.start();
		session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(this.queue);
		MessageConsumer consumer = session.createConsumer(destination);
		consumer.setMessageListener(this.messageListener);
	}
	
	public void shutdown(){
		logger.info("shutdown JMSConsumer");
		try {
			if (session != null) {
				session.close();
				session=null;
			}
			if (connection != null) {
				connection.close();
				connection=null;
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	public void onException(JMSException exception) {
		logger.error("connection error", exception);
		// TODO 在服务器恢复后,自动重新连接
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public int getQueuePrefetch() {
		return queuePrefetch;
	}

	public void setQueuePrefetch(int queuePrefetch) {
		this.queuePrefetch = queuePrefetch;
	}

}
