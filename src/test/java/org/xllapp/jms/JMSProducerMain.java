package org.xllapp.jms;

import java.util.HashMap;
import java.util.Map;

import org.xllapp.jms.JMSProducer;

/**
 * 
 * 
 * @Copyright: Copyright (c) 2008 FFCS All Rights Reserved
 * @Company: 北京福富软件有限公司
 * @author 陈作朋 Aug 19, 2013
 * @version 1.00.00
 * @history:
 * 
 */
public class JMSProducerMain {

	public static void main(String[] args) throws InterruptedException {
//		while (true) {
//		JMSProducer producer = new JMSProducer("failover:(nio://192.168.56.101:60000)?jms.useAsyncSend=true&randomize=false&timeout=3000&trackMessages=true&priorityBackup=true&priorityURIs=nio://192.168.56.101:60000", "system", "manager");
		// JMSProducer producer=new
		// JMSProducer("failover:(nio://192.168.0.211:60001)?jms.useAsyncSend=true&randomize=false&timeout=3000&trackMessages=true&priorityBackup=true&priorityURIs=nio://192.168.0.211:60001","system","manager");
			JMSProducer producer = new JMSProducer("tcp://localhost:60000", "system", "manager");
			producer.setIsLogMessage(false);
//			for (int i = 0; i < 3; i++) {
				final Map<String, Object> map = new HashMap<String, Object>();
				map.put("id", 1);
				map.put("name", "name");
				map.put("password", "password");
				producer.send("TEST", map);
				
				map.put("id", 2);
				producer.sendAsync("TEST", new JMSProducer.MessageBuilder() {
					@Override
					public Object buildMessage() {
						return map;
					}
				});
				
				producer.sendAsync("TEST", new JMSProducer.MessageBuilder() {
					@Override
					public Object buildMessage() {
						return "1";
					}
				});
				
				map.put("id", 3);
				producer.sendSync("TEST", map);
				
				producer.sendSync("TEST", "2");
				
/*				Thread.sleep(5000);
				
			}*/

			
/*			Map<String, Object> map1 = new HashMap<String, Object>();
			map1.put("id", "29");
			map1.put("name", "name");
			map1.put("password", "password");
			producer.send("test", map1);
			
			Map<String, Object> map2 = new HashMap<String, Object>();
			map2.put("id", "3");
			map2.put("name", "name");
			map2.put("password", "password");
			producer.send("test", map2);*/
			
//		}
		// Thread.sleep(99999999);
	}

}
