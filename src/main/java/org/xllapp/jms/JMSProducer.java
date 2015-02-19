package org.xllapp.jms;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 此类用于发生消息.
 * 
 * @author dylan.chen Aug 19, 2013
 * 
 */
public class JMSProducer implements ExceptionListener {

	private final static Logger logger = LoggerFactory.getLogger(JMSProducer.class);

	private final static Logger successMessagelogger = LoggerFactory.getLogger("jms.producer.message.success");

	private final static Logger failedMessagelogger = LoggerFactory.getLogger("jms.producer.message.failed");

	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	
	public final static boolean DEFAULT_USE_ASYNC_SEND_FOR_JMS = true;

	public final static int DEFAULT_DELIVERY_MODE = DeliveryMode.PERSISTENT;

	public final static int DEFAULT_MAX_CONNECTIONS = 1;

	public final static int DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION = 300;

	public final static int DEFAULT_THREAD_POOL_SIZE = 10;

	private boolean useAsyncSendForJMS = DEFAULT_USE_ASYNC_SEND_FOR_JMS;

	private String brokerUrl;

	private String userName;

	private String password;

	private int deliveryMode = DEFAULT_DELIVERY_MODE;

	private int maxConnections = DEFAULT_MAX_CONNECTIONS;

	private int maximumActiveSessionPerConnection = DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION;

	private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

	private ThreadPoolExecutor threadPool;

	private PooledConnectionFactory connectionFactory;

	private boolean isLogMessage = true;

	public JMSProducer(String brokerUrl, String userName, String password) {
		this(brokerUrl, userName, password, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION, DEFAULT_THREAD_POOL_SIZE, DEFAULT_USE_ASYNC_SEND_FOR_JMS, true);
	}

	public JMSProducer(String brokerUrl, String userName, String password, int threadPoolSize) {
		this(brokerUrl, userName, password, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION, threadPoolSize, DEFAULT_USE_ASYNC_SEND_FOR_JMS, true);
	}

	public JMSProducer(String brokerUrl, String userName, String password, int maxConnections, int maximumActiveSessionPerConnection, int threadPoolSize) {
		this(brokerUrl, userName, password, maxConnections, maximumActiveSessionPerConnection, threadPoolSize, DEFAULT_USE_ASYNC_SEND_FOR_JMS, true);
	}

	public JMSProducer(String brokerUrl, String userName, String password, int maxConnections, int maximumActiveSessionPerConnection, int threadPoolSize, boolean useAsyncSendForJMS, boolean isPersistent) {
		this.useAsyncSendForJMS = useAsyncSendForJMS;
		this.deliveryMode = isPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
		this.brokerUrl = brokerUrl;
		this.userName = userName;
		this.password = password;
		this.maxConnections = maxConnections;
		this.maximumActiveSessionPerConnection = maximumActiveSessionPerConnection;
		this.threadPoolSize = threadPoolSize;
		init();
	}

	private void init() {

		// 初始化线程池
		this.threadPool = new ThreadPoolExecutor(1, this.threadPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		this.threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

		// 初始化连接工厂
		ActiveMQConnectionFactory actualConnectionFactory = new ActiveMQConnectionFactory(this.userName, this.password, this.brokerUrl);
		actualConnectionFactory.setUseAsyncSend(this.useAsyncSendForJMS);
		this.connectionFactory = new PooledConnectionFactory(actualConnectionFactory);
		this.connectionFactory.setMaxConnections(this.maxConnections);
		this.connectionFactory.setMaximumActiveSessionPerConnection(this.maximumActiveSessionPerConnection);
	}

	/**
	 * 异步发送消息
	 * 
	 * 此方法已废弃,建议使用sendAsync()或sendSync()系列方法发送消息
	 * 
	 * @param queue
	 * @param object
	 *            只支持Map和String类型对象
	 * 
	 */
	@Deprecated
	public void send(final String queue, final Object object) {
		sendAsyncInternal(queue, new MessageBuilder() {
			@Override
			public Object buildMessage() {
				return object;
			}
		});
	}

	public void sendAsync(String queue, MessageBuilder messageBuilder) {
		sendAsyncInternal(queue, messageBuilder);
	}

	private void sendAsyncInternal(final String queue, final MessageBuilder messageBuilder) {
		if (null == queue || null == messageBuilder) {
			logger.warn("queue and messageBuilder can not be null");
			return;
		}
		
		/**
		 * 复制此方法调用进程的MDC信息到执行进程.
		 */
		final Map<?,?> mdcContextMap=MDC.getCopyOfContextMap();

		this.threadPool.execute(new Runnable() {
			@Override
			public void run() {
				
				/**
				 * 将之前获取的sendAsyncInternal方法的调用进程的MDC信息,保存到任务执行线程中(以便使异步执行的任务日志与其它日志能够串联在一起).
				 */
				MDC.setContextMap(mdcContextMap);
				
				Object message = null;
				try {
					message = messageBuilder.buildMessage();
				} catch (Exception e) {
					logger.error("failure to build message.caused by:"+e.getLocalizedMessage(),e);
				}
				if(null != message){
					sendSync(queue, message);
				}
				
				/**
				 * 清空MDC信息
				 */
				MDC.clear();
				
			}
		});
	}
	
	public void sendSync(String queue, Object object) {
		if (null == queue || null == object) {
			logger.warn("queue and object can not be null");
			return;
		}

		try {
			sendInternal(queue, object);
			logger.debug("sended message[{}] to queue[{}]", object, queue);
			if (this.isLogMessage) {
				successMessagelogger.info("queue[{}]:message[{}]", queue, object);
			}
		} catch (Exception e) {
			logger.error("failure to send message[" + object + "] to queue[" + queue + "].caused by:"+e.getLocalizedMessage(), e);
			if (this.isLogMessage) {
				failedMessagelogger.info("queue[{}]:message[{}],caused by {}", queue, object, e.getLocalizedMessage());
			}
		}
	}

	private void sendInternal(String queue, Object object) throws Exception {
		Connection connection = null;
		Session session = null;
		try {
			connection = this.connectionFactory.createConnection();
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(queue);
			MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(deliveryMode);
			Message message = getMessage(session, object);
			producer.send(message);
		} finally {
			closeSession(session);
			closeConnection(connection);
		}
	}

	@SuppressWarnings("unchecked")
	private Message getMessage(Session session, Object object) throws Exception {
		Message message = null;
		if (object instanceof Map) {
			message = session.createMapMessage();
			Map<String, Object> map = (Map<String, Object>) object;
			if (map != null && !map.isEmpty()) {
				Set<String> keys = map.keySet();
				for (String key : keys) {
					((MapMessage) message).setObject(key, map.get(key));
				}
			}
		} else if (object instanceof String) {
			message = session.createTextMessage((String) object);
		} else {
			String json=OBJECT_MAPPER.writeValueAsString(object);
			message = session.createTextMessage(json);
		}
		return message;
	}

	private void closeSession(Session session) {
		try {
			if (session != null) {
				session.close();
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	private void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	@Override
	public void onException(JMSException exception) {
		logger.error("connection error", exception);
	}

	public void setIsLogMessage(boolean isLogMessage) {
		this.isLogMessage = isLogMessage;
	}
	
	public interface MessageBuilder {
		public Object buildMessage();
	}

}
