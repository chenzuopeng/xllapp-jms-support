package org.xllapp.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xllapp.jms.support.BlockedThreadPoolExecutor;


/**
 *
 *
 * @Copyright: Copyright (c) 2014 FFCS All Rights Reserved 
 * @Company: 北京福富软件有限公司 
 * @author 陈作朋 Jun 27, 2014
 * @version 1.00.00
 * @history:
 * 
 */
public class BlockedThreadPoolExecutorMain {
	
	private final static Logger logger = LoggerFactory.getLogger(BlockedThreadPoolExecutorMain.class);

	public static void main(String[] args) {
		
		final BlockedThreadPoolExecutor blockedThreadPoolExecutor=new BlockedThreadPoolExecutor(1);
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < 3; i++) {
//					System.out.println(Thread.currentThread().getName()+":"+i);
					blockedThreadPoolExecutor.execute(getRunnable(Thread.currentThread().getName(),i,150, true));
					try {
						Thread.sleep(1000*3);
					} catch (InterruptedException e) {
					}
				}
				
			}
		},"t1").start();
		
/*		try {
			Thread.sleep(1000*3);
		} catch (InterruptedException e) {
		}*/
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < 3; i++) {
//					System.out.println(Thread.currentThread().getName()+":"+i);
					blockedThreadPoolExecutor.execute(getRunnable(Thread.currentThread().getName(),i,100, true));
					try {
						Thread.sleep(1000*3);
					} catch (InterruptedException e) {
					}
				}
			}
		},"t2").start();
		
		
/*		try {
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
		}*/
		
	}
	
	public static Runnable getRunnable(final String fqt,final int n,final int i,final boolean sleep){
		return new Runnable() {
			
			@Override
			public void run() {
				logger.debug(Thread.currentThread().getName()+":begin - ["+fqt+"] - "+n);
				if (sleep) {
					try {
						Thread.sleep(1000 * i);
					} catch (InterruptedException e) {
					}
				}
				logger.debug(Thread.currentThread().getName()+":end - ["+fqt+"] - "+n);
			}
			
			public String toString(){
				return "["+fqt+","+n+"]";
			}
		};
	}
	
}
