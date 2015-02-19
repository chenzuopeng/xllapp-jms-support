package org.xllapp.jms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批量获取消息的JMS消费者.
 * 
 * @author dylan.chen Oct 6, 2013
 * 
 */
public class BatchJMSConsumer implements ExceptionListener {

	private final static Logger logger = LoggerFactory.getLogger(BatchJMSConsumer.class);

	private Connection connection;

	private Session session;

	private MessageConsumer consumer;

	private int batchSize;

	private MessageListener messageListener;

	public BatchJMSConsumer(String brokerUrl, String userName, String password, String queue, int batchSize) {
		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(userName, password, brokerUrl);
			this.connection = connectionFactory.createConnection();
			ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
			prefetchPolicy.setQueuePrefetch(this.batchSize = batchSize);
			((ActiveMQConnection) this.connection).setPrefetchPolicy(prefetchPolicy);
			this.connection.setExceptionListener(this);
			this.connection.start();
			this.session = this.connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
			Destination destination = this.session.createQueue(queue);
			this.consumer = this.session.createConsumer(destination);
		} catch (Exception e) {
			throw new RuntimeException("failure to init BatchJMSConsumer", e);
		}

	}

	public List<Message> getMessages() throws Exception {

		if (this.batchSize <= 0) {
			return Collections.emptyList();
		}

		List<Message> messages = new ArrayList<Message>();

		for (int i = 0; i < this.batchSize; i++) {
			Message message = this.consumer.receiveNoWait();
			if (null != message) {
				messages.add(message);
			}
		}

		logger.debug("expected size:{},actual size:{}", this.batchSize, messages.size());

		return messages;
	}

	public void start() {

		logger.info("start BatchJMSConsumer");

		int i = 1;

		while (true) {

			try {

				logger.debug("[{}] handling messages", i);

				List<Message> messages = getMessages();

				if (messages.isEmpty()) {

					logger.debug("[{}] handled 0 messages", i);

					try {
						Thread.sleep(5000);
					} catch (Exception e) {
					}

					i++;

					continue;
				}

				int actualSize = messages.size();

				final CountDownLatch countDownLatch = new CountDownLatch(actualSize);

				long startTime = new Date().getTime();

				for (final Message message : messages) {
					ThreadPool.execute(new Runnable() {
						@Override
						public void run() {
							try {
								BatchJMSConsumer.this.messageListener.onMessage(message);
							} catch (Exception e) {
								logger.error("failure to handle message[" + MessageHelper.toString(message) + "]", e);
							} finally {
								countDownLatch.countDown();
							}
						}
					});
				}

				countDownLatch.await();

				long endTime = new Date().getTime();

				long elapsedTime = endTime - startTime;

				logger.debug("[{}] handled {} messages,elapsed time: {} ms", i, actualSize, elapsedTime);

			} catch (Throwable throwable) {

				logger.error(throwable.getLocalizedMessage(), throwable);

				try {
					Thread.sleep(1000 * 60);
				} catch (Exception e) {
				}

			}

			i++;

		}

	}

	public void shutdown() {
		logger.info("shutdown BatchJMSConsumer");
		try {
			if (this.session != null) {
				this.session.close();
				this.session = null;
			}
			if (this.connection != null) {
				this.connection.close();
				this.connection = null;
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	@Override
	public void onException(JMSException exception) {
		logger.error("connection error", exception);
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

}
