package org.xllapp.jms;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.commons.lang3.time.DateFormatUtils;

/**
 * 辅助类.
 * 
 * @author dylan.chen Oct 6, 2013
 * 
 */
public abstract class MessageHelper {
	
	public static String getMessageID(Message message){
		String messageID=null;
		try {
			messageID=message.getJMSMessageID();
		} catch (JMSException e) {
		}
		return messageID;
	}

	public static String toString(Message message) {
		try {
			if (message instanceof ActiveMQMapMessage) {
				ActiveMQMapMessage mapMessage = (ActiveMQMapMessage) message;
				Map<String, Object> msgContent = mapMessage.getContentMap();
				return format(mapMessage, null != msgContent ? msgContent.toString() : "");
			} else if (message instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) message;
				return format(message, textMessage.getText());
			} else if(message instanceof ObjectMessage) {
				ObjectMessage objectMessage=(ObjectMessage)message;
				Object object=objectMessage.getObject();
				return format(message, null!=object?object.toString():"");
			}else {
				return format(message, message.toString());
			}
		} catch (Exception e) {
			return format(message, "MessageHelper.toString() error,caused by "+e.getLocalizedMessage());
		}
	}

	public static String format(Message message, String messageContent){
		
		String producerId="";
		if(message instanceof ActiveMQMessage){
			producerId=((ActiveMQMessage)message).getProducerId().toString();
		}
		String messageId="";
		try {
			messageId=message.getJMSMessageID();
		} catch (Exception e) {
		}
		String destination="";
		try {
			destination=message.getJMSDestination().toString();
		} catch (Exception e) {
		}
		String strTimestamp="";
		try {
			strTimestamp = DateFormatUtils.format(message.getJMSTimestamp(), "yyyy-MM-dd hh:mm:ss");
		} catch (Exception e) {
			
		}
		
		return "{messageId=" + messageId + ",messageTimestamp=" + strTimestamp + ",producerId="+producerId+",destination="+destination+",messageContent=" + messageContent + "}";
	}

}
 